#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.01

A file system that interacts with an xmlrpc HT.
"""

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
from collections import defaultdict
import socket
from Crypto.Cipher import AES
from Crypto import Random

import random
import sys, pickle, xmlrpclib, fcntl, warnings, exceptions
import md5
import binascii
import base64

BS = 16
password = "FAULT_TOLERANCE"
md5obj = md5.new()
md5obj.update(password)
key = md5obj.digest()
testing = 0 #Set to 1 to enable testing and replicate throughput errors
chanceOfError = .1 #Probability of error occuring [ranges from 0 to 1]

class HtProxy:
  """ Wrapper functions so the FS doesn't need to worry about HT primitives."""
  # A hashtable supporting atomic operations, i.e., retrieval and setting
  # must be done in different operations
  def __init__(self, url):
    self.numServers = 0
    self.rpc = []
    self.AS = AS
    self.LR = LR
    self.size_test = size_test
    self.server_info = {}
    self.files = {}


    #create list of avialable servers
    for x in xrange(len(url)): 
      finalurl= 'http://localhost:'+url[x]
      self.rpc.append(xmlrpclib.Server(finalurl))
      self.numServers += 1
      try:
        self.rpc[x].put(Binary("check"), Binary("temp"), 1, 1)
      except xmlrpclib.Fault:
        continue;
      except socket.error:
        continue;
      name = "server" + str(x) # Server name
      self.server_info[name] = []
      self.server_info[name].append(self.rpc) # The xmlrpc object
      self.server_info[name].append(0)  # Number of files on server
      self.server_info[name].append(11) # Server status
      print "This is AS and LR and size_test:", AS, LR, size_test
  # Retrieves a value from the SimpleHT, returns KeyError, like dictionary, if
  # there is no entry in the SimpleHT

          
  #function to pad the data into chunks of 16 bytes
  def pad(self,data):
    # return data if no padding is required
    if len(data) % BS == 0:
      return data
    # subtract one byte that should be the 0x80
    # if 0 bytes of padding are required, it means only a single \x80 is required.
    padding_required = (BS - 1) - (len(data) % BS)
    data = '%s\x80' % data
    data = '%s%s' % (data, '\x00' * padding_required)
    return data

  def unpad(self,data):
    if not data:
      return data
    data = data.rstrip('\x00')
    if data[-1] == '\x80':
      return data[:-1]
    else:
      return data

  def findads(self,data): #decrypts the AES and returns the unpadded data(CRClen + data + chksum)
    # remove ascii-armouring if present
    if data[0] == '\x00':
        data = data[1:]
    elif data[0] == '\x41':
        data = base64.decodestring(data[1:])
    
    iv = data[:BS]
    data = data[BS:]
    ads = AES.new(key, AES.MODE_CBC, iv)
    data = ads.decrypt(data)
    return self.unpad(data)

  def findcrc(self,val) : #finds crc and appends it to the end of data. Also appends length of crc to start of data
    crcval = binascii.crc32(val) & 0xffffffff
    lencrc = len(str(crcval))
    finalval = str(lencrc) + "|" + val + str(crcval)
      #print 'finalval after crc appending: ', (finalval)
    return finalval

  def findaes(self,final_value, armour = True): #encrypts the padded data(multiple of 16 bytes) in AES and appends the initialisation vector to it for decrypting(used in get call)
    ivp = Random.new().read(BS)
    aes_encryptor = AES.new(key, AES.MODE_CBC, ivp)
    pad_data = self.pad(final_value)
    cipher = aes_encryptor.encrypt(pad_data)  #cipher text
    cipher = ivp + cipher  # pack the initialisation vector in
    # ascii-armouring to preserve encrypted data
    if armour:
      cipher = '\x41' + base64.encodestring(cipher)
    else:
      cipher = '\x00' + cipher
    return cipher

  def __getitem__(self, key):
    rv = self.get(key)
    if rv == None:
      raise KeyError()
    return pickle.loads(rv)
    
  # Stores a value in the SimpleHT
  def __setitem__(self, key, value):
    self.put(key, pickle.dumps(value))

  # Sets the TTL for a key in the SimpleHT to 0, effectively deleting it
  def __delitem__(self, key):
    self.put(key, "", 0)
      
  # Retrieves a value from the DHT, if the results is non-null return true,
  # otherwise false
  def __contains__(self, key):
    return self.get(key) != None

  def get(self, key):
    #convert key into integer and figure out which server it should get data from
    retries = 3
    pending_servers = 0
    while retries > 0:
      retries -= 1
      tempLR = self.LR
      dict1 = {}
        
      while tempLR:
        tempLR -= 1
        serverID = (self.string_hash(key)%(self.size_test))+(pending_servers*self.size_test)
        print "using for get"+ str(serverID)+" "+str(self.string_hash(key))
        pending_servers += 1
        filename = str(key)[1:] + "inter"
        print str(filename)
        try:
          fd = open(filename, 'w+')
        except:
          print "An error occured in open()"
        # Try getting data from the primary server
        servername="server"+str(serverID)
        status = 0
        srvr = self.server_info[servername][0]
        print srvr
        try:
          res = self.rpc[serverID].get(key)
          status = 1
        except xmlrpclib.Fault:
          if (self.server_info[servername][2] >=10): 
            # Try to acquire lock
            try:
              fcntl.flock(fd, fcntl.LOCK_EX )
              print "Lock acquired for : " + filename      
            except:
              print "An error occured in flock"
            # Update the hash table
            self.server_info[servername][2] -= 10
            # Release the lock
            try:
              fcntl.flock(fd, fcntl.LOCK_UN)
              print "Lock released for : " + filename      
            except:
              print "An error occured in flock"
          else:
            pass
        except socket.Fault:
          if (self.server_info[servername][2] >=10): 
            # Try to acquire lock
            try:
              fcntl.flock(fd, fcntl.LOCK_EX )
              print "Lock acquired for : " + filename      
            except:
              print "An error occured in flock"
            # Update the hash table
            self.server_info[servername][2] -= 10
            # Release the lock
            try:
              fcntl.flock(fd, fcntl.LOCK_UN)
              print "Lock released for : " + filename      
            except:
              print "An error occured in flock"
          else:
            pass 
        if status == 1: # Primary server provided data
          try:
            fd.close()
          except:
            print "An error occured in fd.close()"
                 
        if "value" in res:
          datainside = res["value"].data
          if datainside in dict1:
            dict1[datainside] += 1
          else:
            dict1.update({datainside : 1})
	  break
        else:
          continue
            
      dataval = list(dict1.values())
      datakeys = list(dict1.keys())
      if sum(dataval) == 0:
        print "Value does not exist"
        return None
        #AES decryption
      try:
        res_enc_value = datakeys[dataval.index(max(dataval))]
        decryp_data = self.findads(res_enc_value)
        # Now check if checksum of received data is equal to that of its appended checksum
        crclen = int(decryp_data.split("|", 1)[0])
        crc_cksum = decryp_data[-crclen:]
	if testing == 1:
	  if random.randrange(1,(1/chanceOfError),1) == 1:
	    print "Random Error occurred"
            crc_cksum += 1
        recv_data = decryp_data[len(str(crclen))+1 : -crclen]
          #compute checksum of recv_data
        crc_recv_data = binascii.crc32(recv_data) & 0xffffffff
        if str(crc_recv_data) == crc_cksum:
            #no error in data
          return recv_data
        else:
          print "Trying again...retries left :", retries
      except:
        print "Trying again...retries left :", retries
#else:
#       print "Value does not exist"
              #return None
    print "The data has been corrupted..."
    return None

  def put(self, key, val, ttl=10000):
    #convert key into integer and figure out which server it should put data into
    pending_servers = self.LR
    this_key = self.string_hash(key)
    finalvalue = self.findcrc(val) #finds out CRC and returns (len(checksm)+data+checksum)
    cipher = self.findaes(finalvalue) #does AES
    while pending_servers != 0:
      pending_servers -= 1
      serverID = (this_key%(self.size_test))+(pending_servers*self.size_test)
      print "using for put"+ str(serverID)
    #determining what to use between CRC and AES:
      #if(serverID % 2 == 0):
        #find CRC
      self.rpc[serverID].put(Binary(key), Binary(cipher), 10 , ttl)
    return True
    
  def read_file(self, filename):
    pending_servers = self.LR
    this_key = self.string_hash(filename)
    while pending_servers != 0:
      pending_servers -= 1
      serverID = (this_key%(self.AS))+(pending_servers*self.AS)
      self.rpc[serverID].read_file(Binary(filename))
    return self.rpc[serverID].read_file(Binary(filename))

  def write_file(self, filename):
    pending_servers = self.LR
    this_key = self.string_hash(filename)
    while pending_servers != 0:
      pending_servers -= 1
      serverID = (this_key%(self.AS))+(pending_servers*self.AS)
      self.rpc[serverID].write_file(Binary(filename))
    return self.rpc[serverID].write_file(Binary(filename))
  
  #function to convert key into integers. Integers will help store data into different servers
  def string_hash(self,s):
	m = md5.new()
	m.update(s)
	#print (s)
	#print (int(m.hexdigest(),16))
	return int(m.hexdigest(),16)

class Memory(LoggingMixIn, Operations):
  """Example memory filesystem. Supports only one level of files."""
  def __init__(self, ht):
    self.files = ht
    self.fd = 0
    now = time()
    if '/' not in self.files:
      self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
        st_mtime=now, st_atime=now, st_nlink=2, contents=['/'])

  def chmod(self, path, mode):
    ht = self.files[path]
    ht['st_mode'] &= 077000
    ht['st_mode'] |= mode
    self.files[path] = ht
    return 0

  def chown(self, path, uid, gid):
    ht = self.files[path]
    if uid != -1:
      ht['st_uid'] = uid
    if gid != -1:
      ht['st_gid'] = gid
    self.files[path] = ht
  
  def create(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1, st_size=0,
        st_ctime=time(), st_mtime=time(), st_atime=time(), contents='')

    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht

    self.fd += 1
    return self.fd
  
  def getattr(self, path, fh=None):
    if path not in self.files['/']['contents']:
      raise FuseOSError(ENOENT)
    return self.files[path]
  
  def getxattr(self, path, name, position=0):
    attrs = self.files[path].get('attrs', {})
    try:
      return attrs[name]
    except KeyError:
      return ''    # Should return ENOATTR
  
  def listxattr(self, path):
    return self.files[path].get('attrs', {}).keys()
  
  def mkdir(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFDIR | mode),
        st_nlink=2, st_size=0, st_ctime=time(), st_mtime=time(),
        st_atime=time(), contents=[])
    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht

  def open(self, path, flags):
    self.fd += 1
    return self.fd
  
  def read(self, path, size, offset, fh):
    starttime = time()
    ht = self.files[path]
    if 'contents' in self.files[path]:
      print "Read in time: ", time()-starttime
      return self.files[path]['contents']
    return None
  
  def readdir(self, path, fh):
    return ['.', '..'] + [x[1:] for x in self.files['/']['contents'] if x != '/']
  
  def readlink(self, path):
    return self.files[path]['contents']
  
  def removexattr(self, path, name):
    ht = self.files[path]
    attrs = ht.get('attrs', {})
    if name in attrs:
      del attrs[name]
      ht['attrs'] = attrs
      self.files[path] = ht
    else:
      pass    # Should return ENOATTR
  
  def rename(self, old, new):
    f = self.files[old]
    self.files[new] = f
    del self.files[old]
    ht = self.files['/']
    ht['contents'].append(new)
    ht['contents'].remove(old)
    self.files['/'] = ht
  
  def rmdir(self, path):
    del self.files[path]
    ht = self.files['/']
    ht['st_nlink'] -= 1
    ht['contents'].remove(path)
    self.files['/'] = ht
  
  def setxattr(self, path, name, value, options, position=0):
    # Ignore options
    ht = self.files[path]
    attrs = ht.get('attrs', {})
    attrs[name] = value
    ht['attrs'] = attrs
    self.files[path] = ht
  
  def statfs(self, path):
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
  
  def symlink(self, target, source):
    self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
      st_size=len(source), contents=source)

    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(target)
    self.files['/'] = ht
  
  def truncate(self, path, length, fh=None):
    ht = self.files[path]
    if 'contents' in ht:
      ht['contents'] = ht['contents'][:length]
    ht['st_size'] = length
    self.files[path] = ht
  
  def unlink(self, path):
    ht = self.files['/']
    ht['contents'].remove(path)
    self.files['/'] = ht
    del self.files[path]
  
  def utimens(self, path, times=None):
    now = time()
    ht = self.files[path]
    atime, mtime = times if times else (now, now)
    ht['st_atime'] = atime
    ht['st_mtime'] = mtime
    self.files[path] = ht
  
  def write(self, path, data, offset, fh):
    # Get file data
    ht = self.files[path]
    tmp_data = ht['contents']
    toffset = len(data) + offset
    if len(tmp_data) > toffset:
      # If this is an overwrite in the middle, handle correctly
      ht['contents'] = tmp_data[:offset] + data + tmp_data[toffset:]
    else:
      # This is just an append
      ht['contents'] = tmp_data[:offset] + data
    ht['st_size'] = len(ht['contents'])
    self.files[path] = ht
    return len(data)

if __name__ == "__main__":
  if len(argv) < 4:
    print 'usage: %s <mountpoint> <remote hashtable> <Aggregate Server #> <Level of Redundancy>' % argv[0]
    exit(1)
  url =[]
  if len(argv) >=4:
    for x in xrange(2,len(argv)-1):
      url.append(argv[x])
    print url
    print int(len(argv))
    AS = int(len(argv)-3)
    print AS
    LR = int(argv[len(argv)-1])
    print LR
    size_test = AS/LR
    print size_test
    if (AS%LR ==0):
      fuse = FUSE(Memory(HtProxy(url)), argv[1], foreground=True)#, debug=True)
    else:
      print "Error: Need more servers in remote hashtable to provide this FS structure"
    # Create a new HtProxy object using the URL specified at the command-line

