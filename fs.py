import errno
import logging
import os
import stat
import sys
import time

from fuse import FUSE, FuseOSError, Operations

import bs


class BTFS(Operations):

    def __init__(self, rootref=None):
        self.rootref = rootref

    def access(self, path, mode):
        print 'access', path, mode
        if bs.blobref_by_path(self.rootref, path) is None:
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        print 'chmod', path, mode
        return 0

    def chown(self, path, uid, gid):
        print 'chown', path, uid, gid
        return 0

    def create(self, path, mode):
        print 'create', path, mode
        return os.open(path, os.O_WRONLY | os.O_CREAT, mode)

    def getattr(self, path, fh=None):
        print 'getattr', path, fh
        if path == os.sep:
            now = time.time()
            return dict(st_mode=(stat.S_IFDIR | 0755), st_nlink=2)
        attr = bs.attr_by_path(self.rootref, path)
        if not attr:
            raise FuseOSError(errno.ENOENT)
        return attr

    def getxattr(self, path, name, position=0):
        print 'getxattr', path, name, position
        raise FuseOSError(errno.ENOATTR)

    def listxattr(self, path):
        print 'listxattr', path
        raise FuseOSError(errno.ENOATTR)

    def mkdir(self, path, mode):
        print 'mkdir', path, mode
        ref = bs.tree_make(self.rootref, path, mode)
        self.rootref = ref or self.rootref

    def open(self, path, flags):
        # TODO: If the write flag is set, open a temporary copy.
        print 'open', path, flags
        ref = bs.blobref_by_path(self.rootref, path)
        if not ref:
            return 0
        path = bs.get_blobpath(ref)
        return os.open(path, flags)

    def read(self, path, size, offset, fh):
        print 'read', path, size, offset, fh
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, size)

    def readdir(self, path, fh):
        print 'readdir', path, fh
        return ['.', '..'] + bs.files_by_path(self.rootref, path)

    def readlink(self, path):
        print 'readlink', path
        return bs.get_blob(self.rootref, path)

    def removexattr(self, path, name):
        print 'removexattr', path, name
        raise FuseOSError(errno.ENOATTR)

    def rename(self, old, new):
        print 'rename', old, new

    def rmdir(self, path):
        print 'rmdir', path
        pass

    def statfs(self, path):
        print 'statfs', path
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def setxattr(self, path, name, value, options, position=0):
        print 'setxattr', path, name, value, options, position

    def symlink(self, target, source):
        print 'symlink', target, source

    def truncate(self, path, length, fh=None):
        print 'truncate', path, length, fh

    def unlink(self, path):
        print 'unlink', path

    def utimens(self, path, times=None):
        print 'utimens', path, times

    def write(self, path, data, offset, fh):
        print 'write', path, data, offset, fh


def mount(rootref, mountpoint):
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(BTFS(rootref), mountpoint, foreground=True)
