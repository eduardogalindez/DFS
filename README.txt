DFS Proyect Readme
By: Eduardo Galíndez, Ricardo de la Vega
CCOM-4017 Operting Systems
Prof.Jose Ortiz Ubarri




meta-data.py:
	This is the main server, everything is done through this program. It handles the database insertions and the querias, it also handles when a new data node is registered and when an archive is asked it returns the datanodes where the file is. All comunications are done by packets. 

data-node.py:
	This program does 3 functios. First when it is ran and conected to meta-data.py it send the register function so it gets registered in the database as a datanode via the meta-data. The other function is the put function where it recieves datablocks of a file from the copy client and saves it at its designated folder. The get function will send these data chunks back to the copy client.

ls.py:
	This program simply comunicates with the meta-data and asks for a list of all the files in the database. It then simply prints it to the screen.

copy.py:
	This program is a client that eithertakes a file and divides it in the amount of data nodes available and send it to them or it asks to retrieve a file from the meta data server by asking for the chunks of data in the data nodes.


Usage: 

	First things first, we have to crete the database by running python createdb.py

	After the database is created, we can run the meta-data server by running :
		python meta-data.py <port, default=8000>
		

	After the meta-data is up and running we can begin to add as many datanodes as you like by running:	
		python data-node.py <server >< <port> <data path> <metadata port,default=8000>
	
	To add a file to the server is fairly simple, all you have to do is run: 
		python copy.py <source file> <server>:<port>:<dfs file path>

	To view every file already saved in the database, all we have to run is:
		python ls.py <server>:<port, default=8000>
		With the server of the meta data server and its port.

	To use the copy client to copy from the DFS:
		python copy.py <server>:<port>:<dfs file path> <destination file>

Command line attribute's:
	<port, default=8000> : The port is the address where it will be run, if left empty by default it will choose 8000

	<Server>: Where is the server, normally it will be localhost if you run everything on your computer.

	<data path> :Its the path where you want to save the file chunks.

	<metadata port>: The same port where the metdata is running, if you ran the default port you can leave it blank>

	<Source file>: Data path with the file name of the file you want to save.

	<dfs file path>: The name of the file you want to save.

	<destination file>: Path to where you want to put the file you are getting from the meta data.

Who helped:
	