import errno
import logging
import os
import stat
import sys

import fuse

import bs

ENCODING = 'utf-8'


class BTFS(fuse.Operations):

    def __init__(self, store, rootref=None):
        self.store = store
        self.rootref = rootref
        self.fh = 0
        self.fh_refs = {}

    def access(self, path, mode):
        # print 'access', path, mode
        path = path.encode(ENCODING)
        if self.store.blobref_by_path(self.rootref, path):
            return 0
        raise fuse.FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        # print 'chmod', path, mode
        path = path.encode(ENCODING)
        attr = self.store.get_attr(self.rootref, path)
        attr.mod = mode
        self.rootref = self.store.set_attr(self.rootref, path, attr)
        return 0

    def chown(self, path, uid, gid):
        # print 'chown', path, uid, gid
        return 0

    def create(self, path, mode):
        # print 'create', path, mode
        path = path.encode(ENCODING)
        self.fh += 1
        fh = self.fh
        ref = self.fh_refs[fh] = self.store.put_blob('')
        attr = bs.FileAttr(bs.TYPE_BLOB, ref, mode, os.path.split(path)[-1])
        self.rootref = self.store.set_attr(self.rootref, path, attr)
        return fh

    def flush(self, path, fh):
        # print 'flush', path, fh
        pass

    def getattr(self, path, fh=None):
        # print 'getattr', path, fh
        path = path.encode(ENCODING)
        if path == os.sep:
            return dict(st_mode=(stat.S_IFDIR | 0755), st_nlink=2)
        attr = self.store.get_attr(self.rootref, path)
        if not attr:
            raise fuse.FuseOSError(errno.ENOENT)
        return dict(st_mode=attr.mod,
                    st_size=self.store.get_blobsize(attr.ref))

    def getxattr(self, path, name, position=0):
        # print 'getxattr', path, name, position
        raise fuse.FuseOSError(errno.ENOATTR)

    def listxattr(self, path):
        # print 'listxattr', path
        raise fuse.FuseOSError(errno.ENOATTR)

    def mkdir(self, path, mode):
        # print 'mkdir', path, mode
        path = path.encode(ENCODING)
        ref = self.store.put_blob('')
        attr = bs.FileAttr(bs.TYPE_TREE, ref, mode, os.path.split(path)[-1])
        self.rootref = self.store.set_attr(self.rootref, path, attr)

    def open(self, path, flags):
        # TODO: If the write flag is set, open a temporary copy.
        # print 'open', path, flags
        path = path.encode(ENCODING)
        ref = self.store.blobref_by_path(self.rootref, path)
        self.fh += 1
        self.fh_refs[self.fh] = ref
        return self.fh

    def read(self, path, size, offset, fh):
        # print 'read', path, size, offset, fh
        return self.store.get_blob(self.fh_refs[fh], size=size, offset=offset)

    def readdir(self, path, fh):
        # print 'readdir', path, fh
        path = path.encode(ENCODING)
        return ['.', '..'] + map(lambda s: s.decode(ENCODING),
                                 self.store.files_by_path(self.rootref, path))

    def readlink(self, path):
        # print 'readlink', path
        path = path.encode(ENCODING)
        return self.store.get_blob_by_path(self.rootref, path)

    def release(self, path, fh):
        # print 'release', path, fh
        # TODO: add path to root
        del self.fh_refs[fh]

    def removexattr(self, path, name):
        # print 'removexattr', path, name
        raise fuse.FuseOSError(errno.ENOATTR)

    def rename(self, old, new):
        # print 'rename', old, new
        old = old.encode(ENCODING)
        new = new.encode(ENCODING)
        attr = self.store.get_attr(self.rootref, old)
        old_dirpath, old_filename = os.path.split(old)
        new_dirpath, new_filename = os.path.split(new)
        if old_dirpath == new_dirpath:
            attr.name = new_filename
            self.rootref = self.store.set_attr(self.rootref, old, attr)
            return
        new_attr = bs.FileAttr(attr.typ, attr.typ, attr.mod, new_filename)
        attr.ref = None
        # TODO: try to do this without rebuilding the tree twice.
        self.rootref = self.store.set_attr(self.rootref, old, attr)
        self.rootref = self.store.set_attr(self.rootref, new, new_attr)

    def rmdir(self, path):
        # print 'rmdir', path
        path = path.encode(ENCODING)
        attr = self.store.get_attr(self.rootref, path)
        if self.store.get_blobsize(attr.ref):
            raise fuse.FuseOSError(errno.ENOTEMPTY)
        attr.ref = None
        self.rootref = self.store.set_attr(self.rootref, path, attr)

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
        target = target.encode(ENCODING)
        source = source.encode(ENCODING)
        ref = self.store.put_blob(source)
        attr = bs.FileAttr(bs.TYPE_BLOB, ref, stat.S_IFLNK | 0755,
                           os.path.split(target)[1])
        self.rootref = self.store.set_attr(self.rootref, target, attr)

    def truncate(self, path, length, fh=None):
        print 'truncate', path, length, fh
        path = path.encode(ENCODING)
        attr = self.store.get_attr(self.rootref, path)
        attr.ref = self.store.put_blob(self.store.get_blob(attr.ref,
                                                           size=length))
        self.rootref = self.store.set_attr(self.rootref, path, attr)
        if fh is not None:
            self.fh_refs[fh] = attr.ref

    def unlink(self, path):
        path = path.encode(ENCODING)
        attr = self.store.get_attr(self.rootref, path)
        attr.ref = None
        self.rootref = self.store.set_attr(self.rootref, path, attr)

    def utimens(self, path, times=None):
        # print 'utimens', path, times
        pass

    def write(self, path, data, offset, fh):
        # print 'write', path, offset, fh
        path = path.encode(ENCODING)
        attr = self.store.get_attr(self.rootref, path)
        if not attr:
            raise fuse.FuseOSError(errno.ENOENT)
        attr.ref = self.fh_refs[fh] = self.store.put_blob(
            self.store.get_blob(self.fh_refs[fh], size=offset) + data)
        self.rootref = self.store.set_attr(self.rootref, path, attr)
        return len(data)


def mount(store, treeref, mountpoint):
    logging.getLogger().setLevel(logging.DEBUG)
    fuse.FUSE(BTFS(store, treeref), mountpoint, foreground=True)
