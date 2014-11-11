import errno
import logging
import os
import stat
import sys

from fuse import FUSE, FuseOSError, Operations

import bs


class BTFS(Operations):

    def __init__(self, rootref=None):
        self.rootref = rootref
        self.tempfiles = {}
        self.fh = 0
        self.fh_refs = {}

    def access(self, path, mode):
        # print 'access', path, mode
        if path in self.tempfiles or bs.blobref_by_path(self.rootref, path):
            return 0
        raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        # print 'chmod', path, mode
        return 0

    def chown(self, path, uid, gid):
        # print 'chown', path, uid, gid
        return 0

    def close(self, fd):
        # print 'close', fd
        return os.close(fd)

    def create(self, path, mode):
        # print 'create', path, mode
        self.fh += 1
        fh = self.fh
        ref = self.fh_refs[fh] = bs.put_blob('')
        dirpath, filename = os.path.split(path)
        attr = {
            bs.BS_MODE: mode,
            bs.BS_SIZE: 0,
            bs.BS_TYPE: 'blob',
            bs.BS_REF: ref,
            bs.BS_NAME: filename,
        }
        self.rootref = bs.set_attr(self.rootref, path, attr)
        return fh

    def flush(self, path, fh):
        print 'flush', path, fh

    def getattr(self, path, fh=None):
        # print 'getattr', path, fh
        if path in self.tempfiles:
            return self.tempfiles[path]
        if path == os.sep:
            return dict(st_mode=(stat.S_IFDIR | 0755), st_nlink=2)
        attr = bs.get_attr(self.rootref, path)
        if not attr:
            raise FuseOSError(errno.ENOENT)
        return attr

    def getxattr(self, path, name, position=0):
        # print 'getxattr', path, name, position
        raise FuseOSError(errno.ENOATTR)

    def listxattr(self, path):
        # print 'listxattr', path
        raise FuseOSError(errno.ENOATTR)

    def mkdir(self, path, mode):
        # print 'mkdir', path, mode
        ref = bs.tree_make(self.rootref, path, mode)
        self.rootref = ref or self.rootref

    def open(self, path, flags):
        # TODO: If the write flag is set, open a temporary copy.
        # print 'open', path, flags
        ref = bs.blobref_by_path(self.rootref, path)
        self.fh += 1
        self.fh_refs[self.fh] = ref
        return self.fh

    def read(self, path, size, offset, fh):
        print 'read', path, size, offset, fh
        return bs.get_blob(self.fh_refs[fh], size=size, offset=offset)

    def readdir(self, path, fh):
        # print 'readdir', path, fh
        return ['.', '..'] + bs.files_by_path(self.rootref, path)

    def readlink(self, path):
        # print 'readlink', path
        return bs.get_blob(self.rootref, path)

    def release(self, path, fh):
        # print 'release', path, fh
        # TODO: add path to root
        if path in self.tempfiles:
            del self.tempfiles[path]
        del self.fh_refs[fh]

    def removexattr(self, path, name):
        # print 'removexattr', path, name
        raise FuseOSError(errno.ENOATTR)

    def rename(self, old, new):
        # print 'rename', old, new
        pass

    def rmdir(self, path):
        # print 'rmdir', path
        pass

    def statfs(self, path):
        # print 'statfs', path
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def setxattr(self, path, name, value, options, position=0):
        # print 'setxattr', path, name, value, options, position
        pass

    def symlink(self, target, source):
        # print 'symlink', target, source
        pass

    def truncate(self, path, length, fh=None):
        print 'truncate', path, length, fh
        attr = bs.get_attr(self.rootref, path)
        attr[bs.BS_REF] = \
            bs.put_blob(bs.get_blob(attr[bs.BS_REF], offset=length))
        attr[bs.BS_SIZE] = length
        self.rootref = bs.set_attr(self.rootref, path, attr)
        if fh is not None:
            self.fh_refs[fh] = attr[bs.BS_REF]

    def unlink(self, path):
        attr = bs.get_attr(self.rootref, path)
        del attr[bs.BS_REF]
        self.rootref = bs.set_attr(self.rootref, path, attr)

    def utimens(self, path, times=None):
        # print 'utimens', path, times
        pass

    def write(self, path, data, offset, fh):
        # print 'write', path, offset, fh
        ref = self.fh_refs[fh] = bs.put_blob(
            bs.get_blob(self.fh_refs[fh], size=offset) + data)
        print 'add blob', ref
        attr = self.getattr(path, fh)
        attr[bs.BS_REF] = ref
        attr[bs.BS_SIZE] = bs.get_blobsize(ref)
        self.rootref = bs.set_attr(self.rootref, path, attr)
        print 'new root', self.rootref
        return len(data)


def mount(rootref, mountpoint):
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(BTFS(rootref), mountpoint, foreground=True)
