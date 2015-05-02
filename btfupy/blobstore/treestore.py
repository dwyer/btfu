import fnmatch
import os
import time

from . import client

ROOTREF_FILE = '.rootref'

TYPE_BLOB = 'blob'
TYPE_TREE = 'tree'
TYPE_PARENT = 'root'
TYPE_CTIME = 'ctime'


class FileAttr:

    def __init__(self, typ, ref, mod, name):
        self.typ = typ
        self.ref = ref
        self.mod = mod
        self.name = name

    def __str__(self):
        return '%s %s %06o %s' % (self.typ, self.ref, self.mod, self.name)

    @classmethod
    def parse(cls, s):
        typ, ref, mod, name = s.split(' ', 3)
        return cls(typ, ref, int(mod, 8), name)


class TreeStore(client.BlobClient):

    def __init__(self, baseurl, auth_token, root_name='.'):
        super(TreeStore, self).__init__(baseurl, auth_token)
        self.root_path = os.path.abspath(root_name)
        self.key_id = ''
        self.__ignore_patterns = None

    def blobref_by_path(self, ref, path):
        if not path:
            return ref
        if isinstance(path, basestring):
            path = path[1:]
            path = path.split(os.sep) if path else []
            return self.blobref_by_path(ref, path)
        name = path.pop(0)
        for obj in self.get_tree(ref):
            if obj.name == name:
                return self.blobref_by_path(obj.ref, path)
        return ref

    def get_attr(self, ref, path):
        path, name = os.path.split(path)
        ref = self.blobref_by_path(ref, path)
        for attr in self.get_tree(ref):
            if name == attr.name:
                return attr
        return None

    def get_blob_by_path(self, treeref, path):
        blobref = self.blobref_by_path(treeref, path)
        return self.get_blob(blobref)

    def get_tree(self, ref, path=None):
        if path is not None:
            ref = self.blobref_by_path(ref, path)
        for line in self.get_blob(ref).splitlines():
            yield FileAttr.parse(line)

    def ignore_file(self, name):
        if not self.__ignore_patterns:
            self.__ignore_patterns = ['.btfu', ROOTREF_FILE]
            with open(os.path.join(self.root_path, '.btfuignore')) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        self.__ignore_patterns.append(line)
        for pattern in self.__ignore_patterns:
            if fnmatch.fnmatch(name, pattern):
                return True
        return False

    def put_file(self, path):
        if os.path.islink(path):
            blob = os.readlink(path)
        elif os.path.isfile(path):
            f = open(path, 'rb')
            blob = f.read()
            f.close()
        else:
            blob = self.put_tree(path)
        return self.put_blob(blob)

    def put_tree(self, dirpath):
        ls = []
        for name in os.listdir(dirpath):
            if self.ignore_file(name):
                continue
            path = os.path.join(dirpath, name)
            st = os.lstat(path)
            typ = (TYPE_BLOB if os.path.isfile(path) or os.path.islink(path)
                   else TYPE_TREE)
            attr = FileAttr(typ, self.put_file(path), st.st_mode, name)
            ls.append(str(attr))
        return '\n'.join(ls)

    def set_attr(self, rootref, path, new_attr):
        dirpath, filename = os.path.split(path)
        if not filename:
            return new_attr.ref
        exists = False
        tree = []
        for attr in self.get_tree(rootref, dirpath):
            if attr.name == filename:
                if new_attr.ref:
                    tree.append(new_attr)
                exists = True
            else:
                tree.append(attr)
        if not exists:
            tree.append(new_attr)
        tree.sort(lambda a, b: cmp(a.name, b.name))
        ref = self.put_blob('\n'.join(map(str, tree)))
        if dirpath == os.sep:
            return ref
        attr = self.get_attr(rootref, dirpath)
        attr.ref = ref
        return self.set_attr(rootref, dirpath, attr)

    def files_by_path(self, ref, path):
        return [row.name for row in self.get_tree(ref, path)]


class RootAttr:

    def __init__(self, rootref, tree, ctime):
        self.rootref = rootref
        self.tree = tree
        self.ctime = ctime

    def __str__(self):
        ls = []
        if self.rootref is not None:
            ls.append('%s %s' % (TYPE_PARENT, self.rootref))
        ls.append(str(self.tree))
        ls.append('%s %s' % (TYPE_CTIME, self.ctime))
        return '\n'.join(ls)

    @classmethod
    def parse(self, s):
        root = None
        tree = None
        ctime = None
        for line in s.splitlines():
            key, value = line.split(' ', 1)
            if key == TYPE_PARENT:
                root = value
            elif key == TYPE_TREE:
                tree = FileAttr.parse(line)
            elif key == TYPE_CTIME:
                ctime = float(value)
            else:
                continue
        return RootAttr(root, tree, ctime)


class RootStore(TreeStore):

    def get_root(self, ref):
        return RootAttr.parse(self.get_blob(ref))

    def get_roots(self):
        for filename in os.listdir(self.roots_path):
            with open(os.path.join(self.roots_path, filename)) as fp:
                yield fp.read()

    def put_root(self, rootref, filename):
        treeref = self.put_file(filename)
        if rootref is not None:
            root = self.get_root(rootref)
            if treeref == root.tree.ref:
                return rootref
        tree = FileAttr(TYPE_TREE, treeref, os.lstat(filename).st_mode, '.')
        return self.put_blob(str(RootAttr(rootref, tree, time.time())))
