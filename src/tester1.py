import argparse
import xmlrpc.client
import time
if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	args = parser.parse_args()

	hostport = args.hostport

	try:
		client  = xmlrpc.client.ServerProxy('http://' + hostport)
		# Test ping
		#client.surfstore.ping()
		#print("Ping() successful")
		#print(client.surfstore.isLeader())
		#print(client.surfstore.isCrashed())
		#client.surfstore.crash()
		#print(client.surfstore.isCrashed())
		#time.sleep(10)
		#client.surfstore.restore()
		#print(client.surfstore.updatefile("Test.txt", 4, [1,2,3]))
		print(client.surfstore.tester_getversion("Test.txt"))
		#client.surfstore.getfileinfomap()

	except Exception as e:
		print("Client: " + str(e))
