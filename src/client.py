import argparse
import os
import xmlrpc.client
import hashlib
import copy


def downloadfiles(filemap, localfilemap,base,client):
	for keys,values in filemap.items():
			filepath = base + '/' + keys
			if not os.path.exists(filepath):
				nlist = ["0"]
				if keys in localfilemap and localfilemap.get(keys)[1] != nlist: # it means that the user has delete the file
					if values[0] == localfilemap.get(keys)[0] and localfilemap.get(keys)[1] != nlist :
						print('\nDeleting file on the server:' +keys)
						client.surfstore.updatefile(keys, values[0]+1, nlist)
					continue
				elif values[1] != nlist:
					print('\nDownloading: '+keys)
					try:
						f = open(filepath,'wb+')
						for h in values[1]:
							f.write(client.surfstore.getblock(h).data)
					finally:
						f.close()
			else:
				nlist = ["0"]
				if keys in localfilemap:
					if values[0] > localfilemap.get(keys)[0]:
						if values[1] == nlist:
							print('\nDeleting local file: ' + keys)
							os.remove(filepath)
						else:
							print('\nUpdating local file: '+ keys)
							try:
								f = open(filepath,'wb+')
								for h in values[1]:
									f.write(client.surfstore.getblock(h).data)
							finally:
								f.close()
				else:
					print('\nUpdating local file: '+ keys)
					try:
						f = open(filepath,'wb+')
						for h in values[1]:
							f.write(client.surfstore.getblock(h).data)
					finally:
						f.close()

	return True

def uploadfiles(file_list, filemap,localfilemap,base,client):
	for file in file_list:
					if file == "index.txt":
						continue
					filepath = base + "/" + file
					if not os.path.exists(filepath):
						continue
					blockarr = []
					hashlist = []
					fileread = open(filepath,'rb')
					if file in filemap and file in localfilemap:
						if(filemap.get(file)[0] == localfilemap.get(file)[0]):#if remote version is the same as local version
							block = fileread.read(args.blocksize) 
							while block:
								blockarr.append(block)
								hashcode = hashlib.sha256(block).hexdigest()
								hashlist.append(hashcode)
								block = fileread.read(args.blocksize)
							if hashlist == filemap.get(file)[1]:
								continue
							else:
								if client.surfstore.updatefile(file, filemap.get(file)[0] +1, hashlist):
									print("Modifying: "+file)
									for i,h in enumerate(hashlist):
										client.surfstore.putblock(blockarr[i],h)
						else:
							continue
					else:
						print("Uploading "+file)
						block = fileread.read(args.blocksize) 
						while block:
							blockarr.append(block)
							hashcode = hashlib.sha256(block).hexdigest()
							hashlist.append(hashcode)
							client.surfstore.putblock(block,hashcode)
							block = fileread.read(args.blocksize)
						print(client.surfstore.updatefile(file, 1, hashlist))
					fileread.close()
	return True


def syncindex(path,client):
	filemap = client.surfstore.getfileinfomap()
	try:
		f = open(path,"w+")
		for keys, values in filemap.items():
			f.write(keys+' '+str(values[0]))
			for hash_values in values[1]:
				f.write(' '+ hash_values)
			f.write('\n')
	finally:
		f.close()
	return True



if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	parser.add_argument('basedir', help='The base directory')
	parser.add_argument('blocksize', type=int, help='Block size')
	args = parser.parse_args()

	try:
		client  = xmlrpc.client.ServerProxy('http://' + args.hostport)
		# Test ping
		print('Syncing... Hostport: '+ args.hostport+' Basedir: '+args.basedir+' Blocksize: '+str(args.blocksize)+'\n\n')
		servermap = client.surfstore.getfileinfomap()
		localmap = {}
		base = os.path.abspath(args.basedir) #base direction
		if not os.path.isdir(base):os.system("mkdir "+base)
		file_list = os.listdir(base)#files in base direction
		indexpath = base + "/index.txt"	#path for index.txt
		print('Checking index.txt....\n\n')
		if os.path.exists(indexpath):
			print("index.txt exists. modify existing")
			rd = open(indexpath,'r')
			for line in rd.readlines():
				line=line.strip('\n')
				tokens = line.split(' ')
				hashmap = []
				for i, key in enumerate(tokens):
					if i == 0:
						name = tokens[0]
					if i == 1:
						version = int(tokens[1])
					if i > 1: 
						hashmap.append(tokens[i])
				newlist = []
				newlist.append(version)
				newlist.append(hashmap)
				localmap[name] = newlist
			print(localmap)
		else:
			print("index.txt doesn't exist, creating new")
			f = open(indexpath,"w+")
			try:
				for file in file_list:
					if file == "index.txt":
						continue
					filepath = base + "/" + file
					f.write(file+" "+"1")
					blockarr = []
					hashlist = []
					fileread = open(filepath,'rb')
					block = fileread.read(args.blocksize) 
					while block:
						blockarr.append(block)
						hashcode = hashlib.sha256(block).hexdigest()
						hashlist.append(hashcode)
						block = fileread.read(args.blocksize)
					for hashcode in hashlist:
						f.write(" "+ hashcode)
					f.write("\n")
			finally:
				f.close()
		#download non-exist files
		print('\n\nDownloading files from server:\n')
		downloadfiles(servermap,localmap,base,client)
		print('\n\nUploading files to the server:\n')
		uploadfiles(file_list, servermap,localmap, base, client)

		print('\n\nSyncing local index.txt......')
		syncindex(indexpath,client)
	except Exception as e:
		print("Client: " + str(e))
