import socket 
import threading
import os
import time
import hashlib
from json import loads,dumps


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (self.host,self.port)
		self.predecessor = (self.host,self.port)
		self.backup_successor = (self.host, self.port)
		# additional state variables
		self.hash_id = self.hasher(self.host+str(self.port))
		self.start_pinging = True
		
		self.joined = False
		self.lookup_queries = {}
		self.file_lookups = {}
		threading.Thread(target = self.pinger).start()
		



	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		req = client.recv(4096).decode("utf-8")
		req_source_id = int(req.split("|")[0])
		req_type = req.split("|")[1]
		if req_type == "join":
			self.lookup_queries[req_source_id] = (req.split("|")[2], int(req.split("|")[3]))
			self.general_lookup(self.hash_id,req_source_id,self.host,self.port)
		elif req_type == "new_predecessor":
			predecessor = (req.split("|")[2], int(req.split("|")[3]))
			if(self.successor == self.predecessor and self.predecessor == (self.host,self.port)):
				client.send("one_node".encode())
				self.successor = predecessor
			else:
				self.start_pinging = True
			
			self.predecessor = predecessor
			client.send(" ".encode())

		elif req_type == "updated_predecessor_successor":
			pred_succ_essor = self.predecessor[0] + "|" + str(self.predecessor[1]) + "|" + self.successor[0] + "|" + str(self.successor[1])
			client.send(pred_succ_essor.encode()) 
		elif req_type == "your_successor":
			self.successor = (req.split("|")[2], int(req.split("|")[3]))
			self.joined = True
		elif req_type == "lookup":
			self.general_lookup(self.hash_id,req_source_id,req.split("|")[2], int(req.split("|")[3]))
		elif req_type == "lookup_result":
			addr = self.lookup_queries[req_source_id]
			if addr == (self.host,self.port):
				self.file_lookups[req_source_id] = [True,(req.split("|")[2],int(req.split("|")[3]))]
			else:
				reply = str(self.hash_id) + "|" + "your_successor" + "|" +  req.split("|")[2] + "|" + req.split("|")[3]
				addr = self.lookup_queries[req_source_id]
				print("successor returned for",addr,req.split("|")[3])
				#print(addr,reply)
				self.send_message(addr,reply)
		elif req_type == "new_file":
			filename = req.split("|")[2]
			self.files.append(filename)
			client.send("ok".encode("utf-8"))
			self.recieveFile(client,self.host + "_" + str(self.port) + "/" + filename)
		elif req_type == "get_file":
			filename = req.split("|")[2]			
			if filename not in self.files:
				client.send("404".encode("utf-8"))
			else:
				client.send("ok".encode("utf-8"))
				time.sleep(0.1)
				self.sendFile(client,self.host + "_" + str(self.port) + "/" + filename)
		elif req_type == "rehashfiles":
			node_id = req_source_id
			file_hashes = []
			files_to_send = []
			for file in self.files:
				file_hashes.append(self.hasher(file))
			total_files = 0
			for i in range(len(self.files)):
				if self.key > node_id:
					if file_hashes[i] > self.key and self.key > node_id:
						total_files += 1
						files_to_send.append(self.files[i])	
					elif file_hashes[i] < node_id:
						total_files += 1
						files_to_send.append(self.files[i])
				else:
					if self.key < node_id and file_hashes[i] > self.key and file_hashes[i] < node_id:
						total_files += 1
						files_to_send.append(self.files[i])


				# if file_hashes[i] > self.key and self.key > node_id:
				# 	total_files += 1
				# 	files_to_send.append(self.files[i])
				# elif self.key < node_id and (file_hashes[i] > self.key and file_hashes[i] < node_id):

				# elif file_hashes[i] < node_id:
				# 	total_files += 1
				# 	files_to_send.append(self.files[i])
					
			print("rehash send files",len(files_to_send))
			res = str(total_files) + "|" +dumps(files_to_send)
			client.send(str(res).encode("utf-8"))
			print("blocked rehash1")
			for file in files_to_send:
				time.sleep(0.1)
				self.sendFile(client,self.host + "_" + str(self.port) + "/" + file)
				self.files.remove(file)
			print("pass rehash")
		elif req_type == "leaving_update_successor":
			self.successor = (req.split("|")[2], int(req.split("|")[3]))
		elif req_type == "leaving_update_predecessor":
			self.predecessor = (req.split("|")[2], int(req.split("|")[3]))
		elif req_type == "rehashing_to_you":
			files_to_recv = req.split("|")[2]
			files_to_recv = loads(files_to_recv)
			client.send("send".encode("utf-8"))
			print("length at recv:", len(files_to_recv))
			for file in files_to_recv:
				self.files.append(file)
				print("leave-rehash")
				self.recieveFile(client,self.host + "_" + str(self.port) + "/" + file)

	

	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()


	def general_lookup(self,my_id,key,host, port):
		successor_id = self.hasher(self.successor[0]+str(self.successor[1]))
		predecessor_id = self.hasher(self.predecessor[0]+str(self.predecessor[1]))
		
			
		if key > predecessor_id and key < my_id:
			reply = str(key) + "|" + "lookup_result" + "|" + self.host + "|" + str(self.port)
			self.send_message((host,port),reply)
		elif my_id <= predecessor_id and (key < predecessor_id and key < my_id):
			reply = str(key) + "|" + "lookup_result" + "|" + self.host + "|" + str(self.port)
			self.send_message((host,port),reply)
		elif my_id <= predecessor_id and key > predecessor_id:
			reply = str(key) + "|" + "lookup_result" + "|" + self.host + "|" + str(self.port)
			self.send_message((host,port),reply)
		else:
			lookup_req = str(key) + "|" + "lookup" + "|" + host + "|" + str(port)
			self.send_message(self.successor,lookup_req)		

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		if(type(joiningAddr) != str):
			join_req = str(self.hash_id) + "|" + "join" + "|" + self.host + "|" + str(self.port)

			self.send_message(joiningAddr,join_req)
			while not self.joined:
				pass
			
			if self.port == 63001:
				print("join")
			self.update_predecessor()
			time.sleep(0.5)
			rehash_req = str(self.hash_id) + "|rehashfiles"
			soc = socket.socket()
			soc.connect(self.successor)
			soc.send(rehash_req.encode("utf-8"))
			print("blocked rehash2")
			res = soc.recv(1024).decode("utf-8")
			print("/blocked rehash2")
			res = res.split("|")
			total_files = int(res[0])
			files = loads(res[1])
			print("rehash recv files",len(files))
			for file in files:
				self.recieveFile(soc,self.host + "_" + str(self.port) + "/" + file)
				self.files.append(file)


			
		

	def update_predecessor(self):
		req = str(str(self.hash_id) + "|new_predecessor|" + self.host + "|" + str(self.port))
		res = self.send_message_reply(self.successor,req)
		if res == "one_node":
			self.predecessor = self.successor
			
	#6435
	def send_message_reply(self, addr, message):

		soc = socket.socket()
		soc.connect(addr)
		soc.send(message.encode("utf-8"))
		reply = soc.recv(1024).decode("utf-8")
		return reply
		
	def send_message(self, addr, message):
		soc = socket.socket()
		soc.connect(addr)
		soc.send(message.encode("utf-8"))
	

	def pinger(self):
		while self.start_pinging:
			time.sleep(0.5)
			if self.successor != (self.host,self.port):

				req = str(self.hash_id) + "|updated_predecessor_successor"
				predecessor = self.send_message_reply(self.successor,req)
				if (self.host,self.port) != (predecessor.split("|")[0],int(predecessor.split("|")[1])):
					self.successor = (predecessor.split("|")[0],int(predecessor.split("|")[1]))
					self.backup_successor = (predecessor.split("|")[2],int(predecessor.split("|")[3]))
					self.update_predecessor()
			elif self.successor != (self.host,self.port):
				pass





	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		file_hash = self.hasher(fileName)
		self.lookup_queries[file_hash] = (self.host,self.port)
		self.file_lookups[file_hash] = [False,()]
		self.general_lookup(self.hash_id,file_hash,self.host,self.port)
		while self.file_lookups[file_hash][0] != True:
			pass

		soc = socket.socket()
		soc.connect(self.file_lookups[file_hash][1])
		req = str(file_hash) + "|new_file|" + fileName
		soc.send(req.encode("utf-8"))
		print("put block")
		soc.recv(1024)
		print("/put block")
		self.sendFile(soc,fileName)
		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''
		file_hash = self.hasher(fileName)
		self.lookup_queries[file_hash] = (self.host,self.port)
		self.file_lookups[file_hash] = [False,()]
		self.general_lookup(self.hash_id,file_hash,self.host,self.port)
		while self.file_lookups[file_hash][0] != True:
			print("blocked in get1")
			pass

		soc = socket.socket()
		soc.connect(self.file_lookups[file_hash][1])
		req = str(file_hash) + "|get_file|" + fileName
		soc.send(req.encode("utf-8"))
		print("get2")

		avail = soc.recv(1024)
		print("/get2")
		if avail.decode("utf-8") == "404":
			return None
		self.recieveFile(soc,fileName)
		return fileName
		



		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''
		self.start_pinging = False
		time.sleep(0.5)
		msg = str(self.hash_id) + "|leaving_update_successor|" + self.successor[0] + "|" +str(self.successor[1]) 
		self.send_message(self.predecessor,msg)
		msg = str(self.hash_id) + "|leaving_update_predecessor|" + self.predecessor[0] + "|" +str(self.predecessor[1]) 
		self.send_message(self.successor,msg)
		
		soc = socket.socket()
		soc.connect(self.successor)
		msg = str(self.hash_id) + "|rehashing_to_you|" + dumps(self.files)
		soc.send(msg.encode("utf-8"))
		print("leave block")
		soc.recv(1024)
		print("/leave block")
		print("length at send:", len(self.files))
		for file in self.files:
			time.sleep(0.1)
			self.sendFile(soc,self.host + "_" + str(self.port) + "/" + file)

	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		print("send block")

		soc.recv(1024).decode('utf-8')
		print("/send block")

		with open(fileName, "rb") as file:
			print("send block1")

			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				print("send block2")

				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		print("recv block")
		fileSize = int(soc.recv(1024).decode('utf-8'))
		print("/recv block")
		soc.send("ok".encode('utf-8'))		

		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			print("recv block1")

			contentChunk = soc.recv(1024)
			print("/recv block1")
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

		
