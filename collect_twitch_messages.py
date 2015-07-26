from twisted.words.protocols import irc
from twisted.internet import reactor, protocol, task
import pdb
import requests
import re
import datetime

from env_variables import *

class DataHandler():

	def __init__(self):
		self.payload = []
		self.payload_threshold = 100

	def processTwitchMessage(self, message, user, channel):
		
		# ignore 1 character messages and messages longer than 50 characters (crude spam filter) 

		if (len(message) > 50) | (len(message) == 1):
			return

		channel_name = channel.split('#')[1]
		username = user.split('!')[0]
		time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

		self.payload.append((channel_name, username, message, time))

		print len(self.payload), channel, username, message

		if len(self.payload) > self.payload_threshold - 1:
			self.dump_payload()

	def dump_payload(self):

		data_string = self.construct_bulk_string()
		del self.payload[:]
		url_string = 'http://localhost:9200/_bulk'
		r = requests.post(url_string, data=data_string)
		print 'Payload dumped!'

	def construct_bulk_string(self):
		
		output = ''

		for i, data in enumerate(self.payload):
			output +=  '{ "index" : { "_index" : "' + data[0] + '__' + data[3].split(" ")[0] +'", "_type" : "message" }}\n' + '{ "username": "' + data[1] + '", "content": "' + data[2] + '", "time": "' + data[3] + '" }\n'

		return output

class ChannelListener(irc.IRCClient):

	nickname = NICKNAME
	password = PASSWORD
	running_streams = []

	def connectionMade(self):
		print 'connection made!'
		irc.IRCClient.connectionMade(self)
		self.data_handler = DataHandler()

	def connectionLost(self, reason):
		print 'connection lost!'
		irc.IRCClient.connectionLost(self, reason)

	def signedOn(self):
		"""Called when bot has succesfully signed on to server."""
		print 'Getting streams...'
		current_streams = self.getStreams()
		for stream_name in current_streams:
			self.running_streams.append(stream_name)
			self.join(stream_name)
		lc = task.LoopingCall(self.updateStreams)
		lc.start(30)

		print 'signed on, joining channel!'

	def joined(self, channel):
		"""This will get called when the bot joins the channel."""
        print 'joined'

	def privmsg(self, user, channel, msg):
		self.data_handler.processTwitchMessage(msg, user, channel)

	def updateStreams(self):
		current_streams = self.getStreams()
		print "running streams: " + " ".join(self.running_streams)
		current_stream_set = set(current_streams)
		running_stream_set = set(self.running_streams)

		streams_to_join = current_stream_set - running_stream_set
		streams_to_leave = running_stream_set - current_stream_set

		print  "ONLINE STREAMERS: " + " ".join(current_streams)

		if len(streams_to_join) != 0:
			# join new channels
			for stream in streams_to_join:
				print "NOW COLLECTING FROM " + stream
				self.running_streams.append(stream)
				self.join(stream)

		if len(streams_to_leave) != 0:
			# leave old channels
			for stream in streams_to_leave:
				print stream + " HAS GONE OFFLINE - COLLECTION FINISHED"
				self.running_streams.remove(stream)
				self.leave(stream)

	def getStreams(self):
		# queries twitch api in order to find out which followed streams are currently online
		url_string = ACTIVE_STREAM_API_ENDPOINT
		r = requests.get(url_string)

		data = r.json()
		time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

		channel_array = []
		bulk_data_string = ''

		if 'streams' in data:
			for stream in data['streams']:
				channel_array.append(stream['channel']['name'].encode('utf-8'))
				bulk_data_string +='{ "index" : { "_index" : "' + stream['channel']['name'].encode('utf-8') + '__' + time.split(" ")[0] +'", "_type" : "stats" }}\n' + '{ "viewers": "' + str(stream['viewers']) + '", "time": "' + time + '"}\n'

		# also dump viewer count to elasticsearch while stream metadata is in scope
		
		r = requests.post('http://localhost:9200/_bulk', data=bulk_data_string)

		return channel_array

class ChannelListenerFactory(protocol.ClientFactory):

	prot = ChannelListener()

	def buildProtocol(self, addr):
		self.prot.factory = self
		return self.prot

	def clientConnectionLost(self, connector, reason):
		"""If we get disconnected, reconnect to server."""
		print 'clientconnectionlost TRIGGERED'
		connector.connect()

	def clientConnectionFailed(self, connector, reason):
		print "connection failed:", reason
		reactor.stop()

if __name__ == '__main__':
 
 	f = ChannelListenerFactory()

	# connect factory protocol and application to this host and port
	reactor.connectTCP("irc.twitch.tv", 6667, f)
	
	# run bot
	reactor.run()



# connect to twitch > signedOn - get channels, update channel attribute, join channels, run check every 
# >10s> 