#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import sys, pickle, xmlrpclib
from xmlrpclib import Binary
from autolog import *
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
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
  def __getattribute__(self, name):
        returned = object.__getattribute__(self, name)
        if inspect.isfunction(returned) or inspect.ismethod(returned):
            print 'in HtProxy called: ', returned.__name__
        return returned
HtProxy = logclass(HtProxy, logMatch='get|put|__init__|__getitem__|__setitem__|__delitem__|__contains__')

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, ht):
        self.files = ht
        #self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, contents={})
        print "----------------init-------------"
        print self.files['/']
        print "-------------init-end-------------"

    def chmod(self, path, mode):

        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath
          

        ht = self.files['/']
        reduce(dict.__getitem__, keys, ht)['st_mode'] &= 0770000
        reduce(dict.__getitem__, keys, ht)['st_mode'] |= mode
        self.files['/'] = ht

        return 0


    def chown(self, path, uid, gid):

        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath
          

        ht = self.files['/']
        reduce(dict.__getitem__, keys, ht)['st_uid'] = uid
        reduce(dict.__getitem__, keys, ht)['st_gid'] = gid
        self.files['/'] = ht


    def create(self, path, mode):
        print "----------create---------"+ path
        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath[:-1]
        newkey = dictpath[len(dictpath)-1]                

        ht = self.files['/']
        reduce(dict.__getitem__, keys, ht)[newkey] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                                        st_size=0, st_ctime=time(), st_mtime=time(),
                                                        st_atime=time(), contents='')

        self.fd += 1

        self.files['/'] = ht
        
        print "\nFiles Hashtable:"
        print self.files['/']
        print "-------------create end------------------"
        return self.fd



    def getattr(self, path, fh=None):
        print "----------getattr---------"+ path

        if (path == '/'):
            return self.files['/']

        else:
            dictpath = getDictPath(path)
            print dictpath
            try:
                my_contents=reduce(lambda d, k: d[k], dictpath, self.files['/'])
            except KeyError:
                raise FuseOSError(ENOENT)


            print "--------getattr end-----------"
            return my_contents



    def getxattr(self, path, name, position=0):

        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath
        attrs = reduce(dict.__getitem__, keys, self.files['/']).get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath
        attrs = reduce(dict.__getitem__, keys, self.files['/']).get('attrs', {})        
        return attrs.keys()

    def mkdir(self, path, mode):
        print "----------mkdir---------"+ path
        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath[:-1]
        newkey = dictpath[len(dictpath)-1]                

        ht = self.files['/']
        reduce(dict.__getitem__, keys, ht)[newkey] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                                        st_size=0, st_ctime=time(), st_mtime=time(),
                                                        st_atime=time(), contents={})

        keys_parent = dictpath[:-2]
        reduce(dict.__getitem__, keys_parent, ht)['st_nlink'] +=1

        self.files['/'] = ht
        
        print "\nFiles Hashtable:"
        print self.files['/']
        print "-------------mkdir end------------------"

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):

        print "----------read---------"+ path
        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath

        return reduce(dict.__getitem__, keys, self.files['/'])['contents'][offset:offset + size]

    def readdir(self, path, fh):
        print "READDIR path: " + path
        if (path == '/'):
            return ['.', '..'] + [x[0:] for x in self.files['/']['contents'] ]
        else:
            dictpath = getDictPath(path)
            keys = dictpath
            return ['.', '..'] + [x[0:] for x in reduce(dict.__getitem__, keys, self.files['/'])['contents'] ]
            


    def readlink(self, path):

        dictpath = getDictPath(path)

        keys = dictpath
        return reduce(dict.__getitem__, keys, self.files['/'])['contents']


    def removexattr(self, path, name):

        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath
        attrs = reduce(dict.__getitem__, keys, self.files['/']).get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):

        dictpath_old = getDictPath(old)
        dictpath_new = getDictPath(new)
        
        keys_old = dictpath_old[:-1]
        File_Dir_Name_old= dictpath_old[len(dictpath_old)-1]
        keys_new = dictpath_new[:-1]
        File_Dir_Name_new= dictpath_new[len(dictpath_new)-1]

        ht = self.files['/']
                
        reduce(dict.__getitem__, keys_old, ht)[File_Dir_Name_new] =  reduce(dict.__getitem__, keys_old, ht)[File_Dir_Name_old]
        del(reduce(dict.__getitem__, keys_old, ht)[File_Dir_Name_old])
        self.files['/'] = ht
        
        print "\nFiles Hashtable:"
        print self.files['/']
        print "-------------rename end------------------"


    def rmdir(self, path):


        print "----------rmdir---------"+ path
        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath[:-1]
        File_Dir_Name = dictpath[len(dictpath)-1] 
        ht= self.files['/']
        if File_Dir_Name in reduce(dict.__getitem__, keys, ht):
            del(reduce(dict.__getitem__, keys, ht)[File_Dir_Name])

        keys_parent = dictpath[:-2]
        reduce(dict.__getitem__, keys_parent, ht)['st_nlink'] -=1

        self.files['/']=ht
        print "\nFiles Hashtable:"
        print self.files['/']
        print "-------------rmdir end------------------"



    def setxattr(self, path, name, value, options, position=0):
        # Ignore options

        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath
        ht = self.files['/']
        attrs = reduce(dict.__getitem__, keys, ht).get('attrs', {})
        attrs[name] = value
        reduce(dict.__getitem__, keys, ht)['attrs'] = attrs
        self.files[path] = ht



    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):

        dictpath_target = getDictPath(target)
        dictpath_source = getDictPath(source)
        keys_target = dictpath_target[:-1]
        File_Name_target= dictpath_old[len(dictpath_target)-1]
        keys_source = dictpath_source[:-1]
        File_Name_source= dictpath_source[len(dictpath_source)-1]
                
        ht = self.files['/']

        reduce(dict.__getitem__, keys_target, ht)[File_Name_target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
          st_size=len(reduce(dict.__getitem__, keys_source, ht)['contents']), contents=reduce(dict.__getitem__, keys_source, ht)['contents'])


        self.files['/'] = ht


    def truncate(self, path, length, fh=None):

        print "----------truncate---------"+ path
        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath                       

        ht = self.files['/']
        reduce(dict.__getitem__, keys, ht)['contents'] = reduce(dict.__getitem__, keys, ht)['contents'][:length] + '\n'
        reduce(dict.__getitem__, keys, ht)['st_size'] = length
        self.files['/'] = ht
        


        print "\nFiles Hashtable:"
        print self.files['/']
        print "-------------truncate end------------------"


    def unlink(self, path):

        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath[:-1]
        File_Name = dictpath[len(dictpath)-1] 
        ht= self.files['/']
        if File_Name in reduce(dict.__getitem__, keys, ht):
            del(reduce(dict.__getitem__, keys, ht)[File_Name])
        self.files['/'] = ht



    def utimens(self, path, times=None):

        dictpath = getDictPath(path)
        print dictpath
        keys = dictpath
        ht= self.files['/']
        now = time()
        atime, mtime = times if times else (now, now)
        reduce(dict.__getitem__, keys, ht)['st_atime'] = atime
        reduce(dict.__getitem__, keys, ht)['st_mtime'] = mtime

        self.files['/']= ht
        print "\nFiles Hashtable:"
        print self.files['/']
        print "-------------utimens end------------------"

    def write(self, path, data, offset, fh):
        print "----------write---------"+ path
        dictpath = getDictPath(path)
        print dictpath

        keys =dictpath                       

        ht = self.files['/']
        reduce(dict.__getitem__, keys, ht)['contents'] = reduce(dict.__getitem__, keys, ht)['contents'][:offset] + data
        reduce(dict.__getitem__, keys, ht)['st_size'] = len(reduce(dict.__getitem__, keys, ht)['contents'])
        self.files['/'] = ht


        print "\nFiles Hashtable:"
        print self.files['/']
        print "-------------write end------------------"
        return len(data)

def getDictPath(path):
        a=path.split('/')
        splitpath_parent = ['/']        
        splitpath_parent += a[1:len(a)-1] #upto parent directory only        
        File_Dir_Name = a[len(a)-1]        

        dictpath=[]
        
        for name in splitpath_parent[1:]:
            dictpath.append('contents')
            dictpath.append(name)
        
        dictpath.append('contents')    
        dictpath.append(File_Dir_Name)
        return dictpath



Memory = logclass(Memory, logMatch='__init__|chmod|chown|create|getattr|getxattr|listxattr|mkdir|open|read|readdir|readlink|removexattr|rename|rmdir|setxattr|statfs|symlink|truncate|unlink|utimens|write')

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <mountpoint> <remote hashtable port>' % argv[0])
        exit(1)
    port = argv[2]
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(HtProxy(port)), argv[1], foreground=True)