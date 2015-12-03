fusepy: a Python module that provides a simple interface to FUSE
Link: https://github.com/terencehonles/fusepy
Supports file operations: cd , ls, mkdir, rmdir, echo, touch, cat, cp, mv, rm etc.
======

Given an in-memory one level filesystem: memory.py

-Developed a multilevel filesystem that supports multiple levels of directories and files.
- From in memory storage, stored the data and metadata in an XMLRPC server instance and then for load balancing distributed them across multiple servers uniformly.
- To make the system fault tolerant, implemented redundant storage and retrieval using N modular redundancy, applied CRC checksum and stored encrypted data using AES encryption.
