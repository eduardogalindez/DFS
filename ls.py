###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	List client for the DFS
#



import socket
import sys

from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):

	# Contacts the metadata server and ask for list of files.
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((ip, port))

	# Creae a Packet and build it as a list packet
	# then send it to the metadata server
	packet = Packet()
	packet.BuildListPacket()
	s.sendall(packet.getEncodedPacket())

	# Now we take the response of the metadata server
	# the response is a packet with the list of files
	response = s.recv(1024)
	packet.DecodePacket(response)

	# here I get the file array from the packet and
	# I will iterate to display the files and their size
	fileList = packet.getFileArray()
	for f in fileList:
		print f[0] + ' ' + f[1] + 'B'


if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int(server[1])

	if not ip:
		usage()

	client(ip, port)
