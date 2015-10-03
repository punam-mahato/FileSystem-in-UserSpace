#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
#from pprint import pprint
from autolog import *
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
#from pathlib import *

if not hasattr(__builtins__, 'bytes'):
    bytes = str


class HtProxy:
  """ Wrapper functions so the FS doesn't need to worry about HT primitives."""
  # A hashtable supporting atomic operations, i.e., retrieval and setting
  # must be done in different operations
  def __init__(self, port):
    self.server = xmlrpclib.ServerProxy("http://localhost:" + port)
  # Retrieves a value from the SimpleHT, returns KeyError, like dictionary, if
  # there is no entry in the SimpleHT
  
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
    
    res = self.server.get(Binary(key))
    if "value" in res:
      return res["value"].data
    else:
      return None

  def put(self, key, val, ttl=10000):
    
    return self.server.put(Binary(key), Binary(val), ttl)

  #def read_file(self, filename):
  #  return self.rpc.read_file(Binary(filename))

  #def write_file(self, filename):
  #  return self.rpc.write_file(Binary(filename))

def parentDir(self, path):
	parentpath=['/']
	path=path[1:]	
	a=path.split('/')
	parentpath += a[:len(a)-1]
	DS_name = a[len(a)-1]
	
	print"\n"
	print "-----------path resolution-------------"
	print DS_name
	print parentpath
	print "---------------------------------------"
	print"\n"


	
	dict_FS=self.files['/']
	for name in parentpath[1:]:
		dict_FS=dict_FS['contents'][name]
	return [DS_name, dict_FS]





class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, ht):
        self.files = {}
        #self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, contents={})
        #print self.files['/']['contents']

    def chmod(self, path, mode):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        parentdir[contents][DS_Name]['st_mode'] &= 0770000
        parentdir[contents][DS_Name]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]    	
        parentdir[contents][DS_Name]['st_uid'] = uid
        parentdir[contents][DS_Name]['st_gid'] = gid

    def create(self, path, mode):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]    	
    	#print self.files[parentdir]['contents']
    	parentdir['contents'][DS_Name]= dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                						st_size=0, st_ctime=time(), st_mtime=time(),
                                						st_atime=time(), contents='')
    	self.fd += 1    	
    	return self.fd


    def getattr(self, path, fh=None):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]

    	print "----------getattr---------"+ path
    	#print DS_Name
        print "files hashtable:"
        print self.files
        print "\n"
    	#print "parentdir: "
    	#print parentdir

        if DS_Name not in parentdir['contents']:
            raise FuseOSError(ENOENT)


        #print parentdir['contents'][DS_Name]
        return parentdir['contents'][DS_Name]

    def getxattr(self, path, name, position=0):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        attrs = parentdir['contents'][DS_Name].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = parentdir['contents'][DS_Name].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):

    	print "Path to the directory: " + path
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
    	print "----------mkdir---------"+ path
    	print "parentdir: "
    	#print parentdir
    	#print DS_Name
    	parentdir['contents'][DS_Name] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                						st_size=0, st_ctime=time(), st_mtime=time(),
                                						st_atime=time(), contents={})

        parentdir['st_nlink'] += 1
    	#print DS_Name
        print "\nfiles hashtable:"
        print self.files

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        #print parentdir['contents'][DS_Name]['contents'][offset:offset + size]
        return parentdir['contents'][DS_Name]['contents'][offset:offset + size]

    def readdir(self, path, fh):
    	print "READDIR path: " + path
    	if (path == '/'):
    		return ['.', '..'] + [x[0:] for x in self.files['/']['contents'] ]
    	else:
    		val = parentDir(self, path)
    		DS_Name= val[0]
    		parentdir= val[1]
        	#print  [x[1:] for x in self.files if x != '/']
    		return ['.', '..'] + [x[0:] for x in parentdir['contents'][DS_Name]['contents'] ]



    def readlink(self, path):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        return parentdir['contents'][DS_Name]['contents']

    def removexattr(self, path, name):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        attrs = parentdir['contents'][DS_Name].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
    	val_old = parentDir(self, old)
    	DS_Name_old= val_old[0]
    	parentdir_old= val_old[1]
    	val_new = parentDir(self, new)
    	DS_Name_new= val_new[0]
    	parentdir_new= val_new[1]


    	parentdir_old['contents'][DS_Name_new]=parentdir_old['contents'].pop(DS_Name_old)

    def rmdir(self, path):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]



        if DS_Name in parentdir['contents']:
        	parentdir['contents'].pop(DS_Name)
        parentdir['st_nlink'] -= 1


    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        attrs = parentdir['contents'][DS_Name].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        parentdir['contents'][DS_Name] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        parentdir['contents'][DS_Name]['contents'] = source

    def truncate(self, path, length, fh=None):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        parentdir['contents'][DS_Name]['contents'] = parentdir['contents'][DS_Name]['contents'][:length]
        parentdir['contents'][DS_Name]['st_size'] = length

    def unlink(self, path):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
    	parentdir['contents'].pop(DS_Name)


    def utimens(self, path, times=None):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        now = time()
        atime, mtime = times if times else (now, now)
        parentdir['contents'][DS_Name]['st_atime'] = atime
        parentdir['contents'][DS_Name]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
    	val = parentDir(self, path)
    	DS_Name= val[0]
    	parentdir= val[1]
        parentdir['contents'][DS_Name]['contents'] = parentdir['contents'][DS_Name]['contents'][:offset] + data
        parentdir['contents'][DS_Name]['st_size'] = len(parentdir['contents'][DS_Name]['contents'])
        return len(data)


Memory = logclass(Memory, logMatch='__init__|chmod|chown|create|getattr|getxattr|listxattr|mkdir|open|read|readdir|readlink|removexattr|rename|rmdir|setxattr|statfs|symlink|truncate|unlink|utimens|write')

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)
    port = argv[2]
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(HtProxy(port)), argv[1], foreground=True)
