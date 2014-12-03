###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path

from Packet import *

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copu file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""

	# Create a connection to the data server

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)

	# Read file
	# here I read the file and store the contents of the file in a variable named data
	# and store the data length to in a variable called dataLength :D

	theFile = open(path, 'rb')
	data = theFile.read()
	theFile.close()
	dataLength = len(data)

	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 

	packet = Packet()
	packet.BuildPutPacket(fname, dataLength)
	s.sendall(packet.getEncodedPacket())

	# If no error or file exists
	# Get the list of data nodes.
	# Divide the file in blocks
	# Send the blocks to the data servers

	response = s.recv(1024)
	s.close()
	# lets check if the file already exists in the DB
	if response == "DUP":
		# if it does then let the user know
		print "The file already exists"
		return

	# if it isn't in the DB then we get our hands dirty
	else:
		# decode the packet and get the data nodes that the metadata server
		# sent you, also get its size.. this will be usefull later
		packet.DecodePacket(response)
		dataNodes = packet.getDataNodes()
		countDN = len(dataNodes)
		# now we have to divide the file into blocks. To do so we need t know
		# the block size, so that we can divide the file in that size
		blockSize = dataLength/countDN

		# now lets create a list that will hold the data of the file divided
		# representing diferent blocks
		blocks = []

		# if the blicksize is rounded to 0 then we wont divide the file because this means
		# the file is too small and we will send it as a whole
		if blockSize < 1:
			blocks.append(data)
		else:
			# now we can start filling the blocks list with data
			# I'll use range with 3 attributes.. the first one is start
			# from that number... the second one is stop in that number
			# and the third one is count in that interval
			for i in range(0, dataLength, blockSize):
				if len(blocks) + 1 == countDN:
					# in the last iteration we are going to append all as a
					# data block
					blocks.append(data[i:])
				else:
					blocks.append(data[i:i+blockSize])

		# now that we have the blocks list filled with data we can send the blocks
		# to the data nodes
		ctr = 0
		for node in dataNodes:
			# we connect to each datanode in the that the metadata server gave us
			sdn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sdn.connect((node[0], node[1]))
			# now we build a put packet that we will send to the datanode server
			packet.BuildPutPacket(fname, dataLength)
			sdn.sendall(packet.getEncodedPacket())

			#now we wait for the response of the server
			response = sdn.recv(1024)

			if response == "OK":
				block = blocks[ctr]
				blockSize = len(block)

				sdn.sendall(str(blockSize))
				response = sdn.recv(1024)

				# we have to send the block in chunks of data because the buffer size
				# is 1024 so that is what this while loop is for
				while len(block):
					# get a chunk
					dataChunk = block[0:1024]
					# send that chunk
					sdn.sendall(dataChunk)
					# wait for a response
					response = sdn.recv(1024)
					# update the condition
					block = block[1024:]

			else:
				print "Data node never responded.. maybe it died!"

			# here we are waiting for the chunkID
			response = sdn.recv(1024)
			node.append(response)
			ctr+=1
		sdn.close()

	# Notify the metadata server where the blocks are saved.

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)

	# lets create a block packet to send to the metadata server
	# the dataNodes already have the chunkID
	packet.BuildDataBlockPacket(fname, dataNodes)
	#now lets send the packet
	s.sendall(packet.getEncodedPacket())
	s.close()
	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

   	# Contact the metadata server to ask for information of fname
   	# we first connect to the metadata server and build a get packet
   	# with the name of the file we want to copy
   	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
	packet = Packet()
	packet.BuildGetPacket(fname)
	# send the packet to the metadata server
	s.sendall(packet.getEncodedPacket())
	# wait for a response, when you get the response then close the socket
	response = s.recv(1024)
	s.close()

	# If the response is NFOUND is because the metadata server couldnt find
	# the file in the db, so lets notify the user
	if response == "NFOUND":
		print "File not found"
		return
	else:
		# if everything went well with the response then well do the following
		# we will create a file that will contain the data
		theFile = open(path, 'wb')

		# now we will decode the packet and get the list of the datanodes in wich
		# we stored a bloc of the file we want to copy
		packet.DecodePacket(response)
		metaList = packet.getDataNodes()

		# now that we have the datanodes we want to get the block of each datanode
		# so that at the end we can have a copy of the whole file
		for dataNode in metaList:
			# we connect to each datanode in the that the metadata server gave us
			sdn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sdn.connect((dataNode[0], dataNode[1]))

			# now we have to create something to send to de datanode
			# so that it knows what i want from it.. so lets build a 
			# Get Data Block Packet and send the block id that is in the
			# third position in the dataNode list
			packet.BuildGetDataBlockPacket(dataNode[2])
			sdn.sendall(packet.getEncodedPacket())
			# now we wait for the datanode to send us the size of the block
			blockSize = sdn.recv(1024)

			# this part is very important.. this gave us a lot of problems
			# because we didnt have the next sendall and if you dont have that
			# you might screw up things when you recieve the size you HAVE TO respond
			# that you recieved because if you don't then the datanode server will start
			# sending you chunks of the block and you might recieve the wrong data.. this
			# happened to us and we got a chun of the data block as a datasize and the DFS
			# exploded :( .. but we fixed it with this sendall right here :D
			sdn.sendall("recieved the size")

			# now we start to create the copy file.. we start to recieve the chunks
			blockData = ""
			blockData = sdn.recv(1024)
			sdn.sendall("Recieved first chunk")
			# while the length of the copy block isnt the same as the block in the datanode
			# then we will contiue recieving chunks and merging them with the previous chunk
			while len(blockData) < int(blockSize):
				blockData += sdn.recv(1024)
				# dont forget to let the datanode server that you recieved the chunk
				sdn.sendall("recieved another chunk")

			# once I'm out of the while, this means that we have the full block of data
			# and now we have to write this data in the file created at the beguining
			theFile.write(blockData)
			# dont forget to close the socket connection
			sdn.close()
		# when you get here that means that you already have all blocks and you have
		# successfuly created a copy file
		theFile.close()

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)


