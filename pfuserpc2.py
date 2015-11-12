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
  def __getattribute__(self, name):
        returned = object.__getattribute__(self, name)
        if inspect.isfunction(returned) or inspect.ismethod(returned):
            print 'in HtProxy called: ', returned.__name__
        return returned
HtProxy = logclass(HtProxy, logMatch='get|put|__init__|__getitem__|__setitem__|__delitem__|__contains__')

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self,ht):
        self.files = ht
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)

    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        print "Path to the file: " + path
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        self.fd += 1
        return self.fd
        print "files ht"
        print self.files
        print "\n"
        print "data ht"
        print self.data

    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)

        print "files ht"
        print self.files
        print "\n"
        print "data ht"
        print self.data

        print "-----------------------"
        print self.files[path]
        return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        print "files ht"
        print self.files
        print "\n"
        print "data ht"
        print self.data
        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        print "path: " + path
        for x in self.files:
            if (x != '/'):
                print x[1:]
                print "\n ***"
                
        return ['.', '..'] + [x[1:] for x in self.files if x != '/']
        

        

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        print "......................."
        print old
        print new
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)


Memory = logclass(Memory, logMatch='__init__|chmod|chown|create|getattr|getxattr|listxattr|mkdir|open|read|readdir|readlink|removexattr|rename|rmdir|setxattr|statfs|symlink|truncate|unlink|utimens|write')

if __name__ == '__main__':
  if len(argv) < 3:
    print 'usage: %s <mountpoint> <remote hashtable port>' % argv[0]
    exit(1)
  port = argv[2]
  logging.basicConfig()
  logging.getLogger().setLevel(logging.DEBUG)
  
  fuse = FUSE(Memory(HtProxy(port)), argv[1], foreground=True)
