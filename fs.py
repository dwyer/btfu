import errno
import logging
import os
import stat
import sys

from fuse import FUSE, FuseOSError, Operations

from bs import FileAttr, FileStore

bs = FileStore()


class BTFS(Operations):

    def __init__(self, rootref=None):
        self.rootref = rootref
        self.fh = 0
        self.fh_refs = {}

    def access(self, path, mode):
        # print 'access', path, mode
        if bs.blobref_by_path(self.rootref, path):
            return 0
        raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        # print 'chmod', path, mode
        attr = bs.get_attr(self.rootref, path)
        attr.mod = mode
        self.rootref = bs.set_attr(self.rootref, path, attr)
        return 0

    def chown(self, path, uid, gid):
        # print 'chown', path, uid, gid
        return 0

    def create(self, path, mode):
        # print 'create', path, mode
        self.fh += 1
        fh = self.fh
        ref = self.fh_refs[fh] = bs.put_blob('')
        attr = FileAttr(bs.TYPE_BLOB, ref, mode, os.path.split(path)[-1])
        self.rootref = bs.set_attr(self.rootref, path, attr)
        return fh

    def flush(self, path, fh):
        # print 'flush', path, fh
        pass

    def getattr(self, path, fh=None):
        # print 'getattr', path, fh
        if path == os.sep:
            return dict(st_mode=(stat.S_IFDIR | 0755), st_nlink=2)
        attr = bs.get_attr(self.rootref, path)
        if not attr:
            raise FuseOSError(errno.ENOENT)
        return dict(st_mode=attr.mod, st_size=bs.get_blobsize(attr.ref))

    def getxattr(self, path, name, position=0):
        # print 'getxattr', path, name, position
        raise FuseOSError(errno.ENOATTR)

    def listxattr(self, path):
        # print 'listxattr', path
        raise FuseOSError(errno.ENOATTR)

    def mkdir(self, path, mode):
        # print 'mkdir', path, mode
        ref = bs.put_blob('')
        attr = FileAttr(bs.TYPE_TREE, ref, mode, os.path.split(path)[-1])
        self.rootref = bs.set_attr(self.rootref, path, attr)

    def open(self, path, flags):
        # TODO: If the write flag is set, open a temporary copy.
        # print 'open', path, flags
        ref = bs.blobref_by_path(self.rootref, path)
        self.fh += 1
        self.fh_refs[self.fh] = ref
        return self.fh

    def read(self, path, size, offset, fh):
        # print 'read', path, size, offset, fh
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
        del self.fh_refs[fh]

    def removexattr(self, path, name):
        # print 'removexattr', path, name
        raise FuseOSError(errno.ENOATTR)

    def rename(self, old, new):
        # print 'rename', old, new
        attr = bs.get_attr(self.rootref, old)
        old_dirpath, old_filename = os.path.split(old)
        new_dirpath, new_filename = os.path.split(new)
        if old_dirpath == new_dirpath:
            attr.name = new_filename
            self.rootref = bs.set_attr(self.rootref, old, attr)
            return
        new_attr = FileAttr(attr.typ, attr.typ, attr.mod, new_filename)
        attr.ref = None
        # TODO: try to do this without rebuilding the tree twice.
        self.rootref = bs.set_attr(self.rootref, old, attr)
        self.rootref = bs.set_attr(self.rootref, new, new_attr)

    def rmdir(self, path):
        # print 'rmdir', path
        attr = bs.get_attr(self.rootref, path)
        if bs.get_blobsize(attr.ref):
            raise FuseOSError(errno.ENOTEMPTY)
        del attr.ref
        self.rootref = bs.set_attr(self.rootref, path, attr)

    def statfs(self, path):
        # print 'statfs', path
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def setxattr(self, path, name, value, options, position=0):
        # print 'setxattr', path, name, value, options, position
        pass

    def symlink(self, target, source):
        # BUG: "too many levels of symbolic links" when trying to read a linked
        # file.
        # print 'symlink', target, source
        ref = bs.put_blob(source)
        attr = FileAttr(bs.TYPE_BLOB, ref, stat.S_IFLNK | 0755,
                        os.path.split(target)[1])
        self.rootref = bs.set_attr(self.rootref, target, attr)

    def truncate(self, path, length, fh=None):
        print 'truncate', path, length, fh
        attr = bs.get_attr(self.rootref, path)
        attr.ref = bs.put_blob(bs.get_blob(attr.ref, size=length))
        self.rootref = bs.set_attr(self.rootref, path, attr)
        if fh is not None:
            self.fh_refs[fh] = attr.ref

    def unlink(self, path):
        attr = bs.get_attr(self.rootref, path)
        attr.ref = None
        self.rootref = bs.set_attr(self.rootref, path, attr)

    def utimens(self, path, times=None):
        # print 'utimens', path, times
        pass

    def write(self, path, data, offset, fh):
        # print 'write', path, offset, fh
        attr = bs.get_attr(self.rootref, path)
        if not attr:
            raise FuseOSError(errno.ENOENT)
        attr.ref = self.fh_refs[fh] = bs.put_blob(
            bs.get_blob(self.fh_refs[fh], size=offset) + data)
        self.rootref = bs.set_attr(self.rootref, path, attr)
        return len(data)


def mount(rootref, mountpoint):
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(BTFS(rootref), mountpoint, foreground=True)
