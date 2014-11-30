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

	theFile = open(path, 'r')
	data = theFile.read()
	theFile.close()
	dataLength = len(data)

	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 

	packet = new Packet()
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
			sdn.connect((i[0], i[1]))
			# now we build a put packet that we will send to the datanode server
			packet.BuildPutPacket(fname, dataLength)
			sdn.sendall(packet.getEncodedPacket())

			#now we wait for the response of the server
			response = sdn.recv(1024)

			if response = "OK":
				block = blocks[ctr]
				


	# Notify the metadata server where the blocks are saved.

	# Fill code
	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

   	# Contact the metadata server to ask for information of fname

	# Fill code

	# If there is no error response Retreive the data blocks

	# Fill code

    	# Save the file
	
	# Fill code

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


