#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from autolog import *
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
#from pathlib import *

if not hasattr(__builtins__, 'bytes'):
    bytes = str



class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
        self.files = {}
        #self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, contents={})
        #print self.files['/']['contents']

    def chmod(self, path, mode):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        parentdir[contents][File_Dir_Name]['st_mode'] &= 0770000
        parentdir[contents][File_Dir_Name]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]       
        parentdir[contents][File_Dir_Name]['st_uid'] = uid
        parentdir[contents][File_Dir_Name]['st_gid'] = gid

    def create(self, path, mode):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]       
        #print self.files[parentdir]['contents']
        parentdir['contents'][File_Dir_Name]= dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                                        st_size=0, st_ctime=time(), st_mtime=time(),
                                                        st_atime=time(), contents='')
        self.fd += 1        
        return self.fd


    def getattr(self, path, fh=None):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]

        print "----------getattr---------"+ path
        #print File_Dir_Name
        print "files hashtable:"
        print self.files
        print "\n"
        #print "parentdir: "
        #print parentdir

        if File_Dir_Name not in parentdir['contents']:
            raise FuseOSError(ENOENT)


        #print parentdir['contents'][File_Dir_Name]
        return parentdir['contents'][File_Dir_Name]

    def getxattr(self, path, name, position=0):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        attrs = parentdir['contents'][File_Dir_Name].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = parentdir['contents'][File_Dir_Name].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):

        print "Path to the directory: " + path
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        print "----------mkdir---------"+ path
        print "parentdir: "
        #print parentdir
        #print File_Dir_Name
        parentdir['contents'][File_Dir_Name] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                                        st_size=0, st_ctime=time(), st_mtime=time(),
                                                        st_atime=time(), contents={})

        parentdir['st_nlink'] += 1
        #print File_Dir_Name
        print "\nfiles hashtable:"
        print self.files

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        #print parentdir['contents'][File_Dir_Name]['contents'][offset:offset + size]
        return parentdir['contents'][File_Dir_Name]['contents'][offset:offset + size]

    def readdir(self, path, fh):
        print "READDIR path: " + path
        if (path == '/'):
            return ['.', '..'] + [x[0:] for x in self.files['/']['contents'] ]
        else:
            val = parentDir(self, path)
            File_Dir_Name= val[0]
            parentdir= val[1]
            #print  [x[1:] for x in self.files if x != '/']
            return ['.', '..'] + [x[0:] for x in parentdir['contents'][File_Dir_Name]['contents'] ]



    def readlink(self, path):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        return parentdir['contents'][File_Dir_Name]['contents']

    def removexattr(self, path, name):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        attrs = parentdir['contents'][File_Dir_Name].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        val_old = parentDir(self, old)
        File_Dir_Name_old= val_old[0]
        parentdir_old= val_old[1]
        val_new = parentDir(self, new)
        File_Dir_Name_new= val_new[0]
        parentdir_new= val_new[1]


        parentdir_old['contents'][File_Dir_Name_new]=parentdir_old['contents'].pop(File_Dir_Name_old)

    def rmdir(self, path):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]



        if File_Dir_Name in parentdir['contents']:
            parentdir['contents'].pop(File_Dir_Name)
        parentdir['st_nlink'] -= 1


    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        attrs = parentdir['contents'][File_Dir_Name].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        parentdir['contents'][File_Dir_Name] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        parentdir['contents'][File_Dir_Name]['contents'] = source

    def truncate(self, path, length, fh=None):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        parentdir['contents'][File_Dir_Name]['contents'] = parentdir['contents'][File_Dir_Name]['contents'][:length]
        parentdir['contents'][File_Dir_Name]['st_size'] = length

    def unlink(self, path):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        parentdir['contents'].pop(File_Dir_Name)


    def utimens(self, path, times=None):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        now = time()
        atime, mtime = times if times else (now, now)
        parentdir['contents'][File_Dir_Name]['st_atime'] = atime
        parentdir['contents'][File_Dir_Name]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        val = parentDir(self, path)
        File_Dir_Name= val[0]
        parentdir= val[1]
        parentdir['contents'][File_Dir_Name]['contents'] = parentdir['contents'][File_Dir_Name]['contents'][:offset] + data
        parentdir['contents'][File_Dir_Name]['st_size'] = len(parentdir['contents'][File_Dir_Name]['contents'])
        return len(data)

def parentDir(self, path):
    parentpath=['/']
    path=path[1:]   
    a=path.split('/')
    parentpath += a[:len(a)-1]
    File_Dir_Name = a[len(a)-1]   


    #parentpath: (parentpath split) absolute path split by '/', just to navigate to parent directory
    #File_Dir_Name: name of the current file or directory
    #Parent_Dir: parent directory dict of the current file or directory
    
    Parent_Dir=self.files['/']
    for name in parentpath[1:]:
        Parent_Dir=Parent_Dir['contents'][name]
    #print Parent_Dir

    print"\n"
    print "-----------path resolution-------------"
    print "File_Dir_Name: "
    print File_Dir_Name
    print "parentpath split: "
    print parentpath
    print "\nParent_Dir: "
    print Parent_Dir
    print "---------------------------------------"
    print"\n"

    return [File_Dir_Name, Parent_Dir]

Memory = logclass(Memory, logMatch='__init__|chmod|chown|create|getattr|getxattr|listxattr|mkdir|open|read|readdir|readlink|removexattr|rename|rmdir|setxattr|statfs|symlink|truncate|unlink|utimens|write')

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)