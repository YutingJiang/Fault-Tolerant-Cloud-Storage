from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
import xmlrpc.client
import hashlib
import argparse
import threading
import time
import random
from threading import Lock

GlobalMutex = Lock()


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")
    if h in blockstore:
        blockData = blockstore.get(h)
        return blockData
    else:
        return False

# Puts a block
def putblock(b,h):
    """Puts a block"""
    print("PutBlock()")
    blockstore[h] = b
    print(blockstore)
    return True

# Given a list of blocks, return the subset that are on this server
def hasblocks(blocklist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")
    blocks = []
    for block in blocklist:
        if block in blockstore:
            blocks.append(block)
    return blocks


def count_normal(service,i):
    global state_list
    crash_state = True
    try :
        crash_state = service.surfstore.isCrashed()
    except Exception as e:
        print("Server:"  + str(e))
    with GlobalMutex:
        if crash_state == True:
            state_list[i] = 0
        else:
            state_list[i] = 1
            
def sysisvalid():
    Threadlist = []
    global state_list
    while True:
        state_list=[-1]*len(service_list)#-1 means no response,0 crashed,1 normal
        for i in range(len(service_list)):
            try:
                Threadlist.append(threading.Thread(target=count_normal,args=(service_list[i],i,)))
                Threadlist[-1].start() 
            except Exception as e:
                print("Server: " + str(e)) 
        while sum([a for a in state_list if a>0])+1 <= maxnum/2:
            if -1 not in state_list:
                break
        if sum([a for a in state_list if a>0])+1 > maxnum/2:
            break
    
# Retrieves the server's FileInfoMap
def getfileinfomap():
    if cur_state == 0:
        sysisvalid()
        return fileinfomap
    else:
        raise Exception("The server you request is not the leader!")
    

# Update a file's fileinfo entry
def updatefile(filename, version, blocklist):
    global counter
    with GlobalMutex:
        counter = 1
    if cur_state == 0:
        sysisvalid()
        log.append([term,[filename,version,blocklist]])
        if counter > maxnum / 2:
            print("UpdateFile()")
            if  filename in fileinfomap:
                if fileinfomap.get(filename)[0] == version - 1:
                    hashlist = blocklist
                    newlist = []
                    newlist.append(version)
                    newlist.append(hashlist)
                    fileinfomap[filename] = newlist
                else:
                    busy = False
                    return False
            else:
                hashlist = []
                hashlist = blocklist
                newlist = []
                newlist.append(version)
                newlist.append(hashlist)
                fileinfomap[filename] = newlist
            return True
    else:
        raise Exception("The server you request is not the leader!")

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    return cur_state == 0

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global cur_state
    cur_state = -1
    return cur_state == -1

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    global cur_state
    cur_state =  2
    return cur_state != -1



# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    return cur_state == -1

# Requests vote from this server to become the leader
def requestVote(serverid, _term,last_index,last_term):
    """Requests vote to be the leader"""
    global cur_state
    if cur_state != -1:
        print("Request vote from server:", serverid)
        global term
        global has_voted
        global vote_for
        global start
        start = int(time.time() * 1000)
        if vote_for == serverid:
            has_voted = False
        if cur_state < 2 and _term <= term:
            print("Denying vote to server:", serverid)
            return False
        else:
            if _term > term and last_term >= log[-1][0] and last_index >= len(log):
                print("Granting vote to server:", serverid)
                vote_for = serverid
                has_voted = True
                term = _term
                return True
            else:
                print("Denying vote to server:", serverid)
                return False

# Updates fileinfomap
#Return 0: heartbeat; 1: entries; 2:commited
def appendEntries(serverid, _term, _log, _commitIndex):
    """Updates fileinfomap to match that of the leader"""
    global cur_state
    if cur_state != -1:
        global has_voted
        global term
        global fileinfomap
        global start
        global cur_leader
        global commitIndex
        cur_leader = serverid
        start = int(time.time() * 1000)
        cur_state = 2
        term = _term
        has_voted = False
        if commitIndex == _commitIndex and len(log) == len(_log):
            return 0
        else:
            if len(_log) > len(log):
                log.append(_log[len(log)])
            if _commitIndex > commitIndex: 
                commitIndex +=1
                state = 2
                filename = log[commitIndex][0]
                version = log[commitIndex][1]
                blocklist = log[commitIndex][2]
                updatelocalfile(filename, version, blocklist)
            else:
                state = 1
        return state

def updatelocalfile(filename, version, blocklist):
    if  filename in fileinfomap:
        if fileinfomap.get(filename)[0] == version - 1:
            hashlist = blocklist
            newlist = []
            newlist.append(version)
            newlist.append(hashlist)
            fileinfomap[filename] = newlist
        else:
            return False
    else:
        hashlist = []
        hashlist = blocklist
        newlist = []
        newlist.append(version)
        newlist.append(hashlist)
        fileinfomap[filename] = newlist
    return True

def tester_getversion(filename):
    if filename in fileinfomap:
        return fileinfomap[filename][0]
    else:
        return "None"

# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)


    return maxnum, host, port


def count_election(service):
    global count
    get_vote = False
    try :
        get_vote = service.surfstore.requestVote(servernum,term,len(log),log[-1][0])
        print(get_vote,service)
    except Exception as e:
        print("Server:"  + str(e))
    with GlobalMutex:
        if get_vote:
            count += 1
        

def heartbeat(service):
    global counter
    state = -1
    try :
        state = service.surfstore.appendEntries(servernum,term,log, commitIndex)
    except Exception as e:
        pass
    with GlobalMutex:
        if state == 1:
            counter +=1

def start_leading():
    while cur_state != -1:  
        Threadlist = []
        for service in service_list:
            try:
                Threadlist.append(threading.Thread(target=heartbeat,args=(service,)))
                Threadlist[-1].start() 
            except Exception as e:
                print("Server: " + str(e))
        time.sleep(0.25)



def init_election():
    print("Starting  Election")
    global cur_state
    global term
    global start
    global count
    global has_voted
    has_voted = True
    count = 1
    start = int(time.time() * 1000)
    term += 1
    cur_state = 1
    Threadlist = []
    for service in service_list:
        try:
            Threadlist.append(threading.Thread(target=count_election,args=(service,)))
            Threadlist[-1].start() 
        except Exception as e:
            print("Server: " + str(e))
    while count <= maxnum/2 and int(time.time() *1000) - start < timeout:
        continue
    if count > maxnum/2 and cur_state==1:
        cur_state = 0
        start_leading()
    return True

def init_server():
    global term
    global service_list
    global cur_state
    global prev_state
    global serverlist
    global maxnum
    global host
    global port
    global has_voted
    global vote_for
    global commitIndex
    vote_for = -1
    commitIndex = 1
    has_voted = False
    global log
    log = [[term,[]]]
    term = 0
    cur_state = 2
    prev_state = 2
    maxnum, host, port = readconfig(config, servernum)
    for servers in serverlist:
        service = xmlrpc.client.ServerProxy('http://' + servers)
        service_list.append(service)


def Timer():
    global cur_state
    if cur_state != -1:
        global start
        global has_voted
        start = int(time.time() * 1000)
        global timeout
        timeout = random.randint(350,500)
        while cur_state != -1:
            if int(time.time() *1000) - start > timeout and (cur_state  == 1 or cur_state == 2) :
                has_voted = False
                init_election()
                timeout = random.randint(350,500)
            




def start_server():
    try:
        print("Attempting to start XML-RPC Server...")
        print(host, port, servernum)
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))



if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum
    except Exception as e:
        print("Server: " + str(e))
    # server list has list of other servers
    serverlist = []
        #current term
    log = []
    term = 0
    cur_leader = -1
        #list of RPC services of other servers
    service_list = []
    blockstore = {}
    fileinfomap = {}
        #State: crashed -1, leader 0, candidate 1, follower 2
    cur_state = 2 # start as follower
    prev_state = 2
    vote_for = -1
        # maxnum is maximum number of servers
    maxnum = 0
    host = ""
    port = 0
    init_server()
    print(serverlist)
    print(service_list)
    th1 = threading.Thread(target=Timer)
    th1.start()
    start_server()
