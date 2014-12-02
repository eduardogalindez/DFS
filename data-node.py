###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path

def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((meta_ip, meta_port))

	try:
		response = "NAK"
		sp = Packet()
		while response == "NAK":
			sp.BuildRegPacket(data_ip, data_port)
			s.sendall(sp.getEncodedPacket())
			response = s.recv(1024)
			print response

			if response == "DUP":
				print "Duplicate Registration"

		 	if response == "NAK":
				print "Registratation ERROR"

			if response == "ACK":
				print "Registratation perfect"

	finally:
		s.close()
	

class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

	def handle_put(self, p):

		"""Receives a block of data from a copy client, and 
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""

		fname, fsize = p.getFileInfo()

		self.request.send("OK")
		BSize= int(self.request.recv(1024))
		self.request.send("Got data size")

		# Generates an unique block id.
		blockid = str(uuid.uuid1())

		DBlock = ""
		DBlock= self.request.recv(1024)
		self.request.sendall("Got first chunk")
		while len(DBlock) < BSize:
			DBlock+= self.request.recv(1024)
			self.request.send("recieved another chunk")

		# Open the file for the new data block.  
		# Receive the data block.
		# Send the block id back
		NewFile = open(DATA_PATH+blockid, 'wb')
		self.request.sendall(blockid)
		NewFile.write(DBlock)
		NewFile.close
	def handle_get(self, p):
		
		# Get the block id from the packet
		blockid = p.getBlockID()


		# Read the file with the block id data
		# Send it back to the copy client.
		IDFile = open(DATA_PATH+blockid, 'rb')
		data= IDFile.read()
		IDFile.close()
		datalen = len(data)
		self.request.sendall(str(datalen))
		while len(data):
			# get a chunk
			dataChunk = data[0:1024]
			# send that chunk
			sdn.sendall(dataChunk)
			# wait for a response
			response = sdn.recv(1024)
			# update the condition
			data = block[1024:]
		print"Sent all data blocks"
		

	def handle(self):
		msg = self.request.recv(1024)
		print msg, type(msg)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			self.handle_get(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 4:
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]

		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])

		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH
			usage()
	except:
		usage()


	register("localhost", META_PORT, HOST, PORT)
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
 	server.serve_forever()
