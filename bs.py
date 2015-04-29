import distutils.dir_util
import fnmatch
import hashlib
import os
import stat
import uuid

BLOBSTORE_NAME = '.btfu'
HOME_DIR = os.environ['HOME']


class BlobStore(object):

    def __init__(self):
        self.repo_path = os.path.join(HOME_DIR, BLOBSTORE_NAME)
        self.blobs_path = os.path.join(self.repo_path, 'blobs')
        self.roots_path = os.path.join(self.repo_path, 'roots')

    def blobref(self, blob):
        return 'sha1-%s' % hashlib.sha1(blob).hexdigest()

    def get_blob(self, ref, path=None, size=-1, offset=0):
        if path is not None:
            ref = self.blobref_by_path(ref, path)
        with open(self.get_blobpath(ref), 'rb') as f:
            f.seek(offset)
            return f.read(size)

    def get_blobpath(self, ref, split=False):
        a, b = ref.split('-', 1)
        return os.path.join(self.blobs_path, a, b[0:2], b[2:4], b[4:])

    def get_blobsize(self, ref):
        return os.stat(self.get_blobpath(ref)).st_size

    def put_blob(self, blob):
        ref = self.blobref(blob)
        blobpath = self.get_blobpath(ref)
        if not os.path.exists(blobpath):
            distutils.dir_util.mkpath(os.path.dirname(blobpath))
            with open(blobpath, 'wb') as f:
                f.write(blob)
            os.chmod(blobpath, 0400)
        return ref

    def setup(self):
        try:
            os.mkdir(self.repo_path)
            os.mkdir(self.blobs_path)
            os.mkdir(self.roots_path)
        except OSError, e:
            print e


class CloudStore(BlobStore):

    def __init__(self):
        super(CloudStore, self).__init__()
        self.gpg = gnupg.GPG(
            '/usr/local/bin/gpg', gnupghome=os.path.join(HOME_DIR, '.gnupg'))

    def decrypt(self, blob):
        return str(self.gpg.decrypt(blob, passphrase=os.environ['BTFU_PASS']))

    def encrypt(self, blob):
        return str(self.gpg.encrypt(blob, self.key_id, armor=False))


class FileStore(BlobStore):

    MODE = 'st_mode'
    TYPE = 'bs_type'
    REF = 'bs_ref'
    NAME = 'bs_name'

    TYPE_BLOB = 'blob'
    TYPE_TREE = 'tree'
    TYPE_PARENT = 'root'
    TYPE_CTIME = 'ctime'

    def __init__(self, root_name='.'):
        super(FileStore, self).__init__()
        self.root_path = os.path.abspath(root_name)
        self.ATTR_KEYS = [self.MODE, self.TYPE, self.REF, self.NAME]
        self.key_id = ''
        self.__ignore_patterns = None

    def attr_to_str(self, attr):
        return '%06o %s %s %s' % tuple(attr[key] for key in self.ATTR_KEYS)

    def blobref_by_path(self, ref, path):
        if not path:
            return ref
        if isinstance(path, basestring):
            path = path[1:]
            path = path.split(os.sep) if path else []
            return self.blobref_by_path(ref, path)
        name = path.pop(0)
        for obj in self.get_tree(ref):
            if obj[self.NAME] == name:
                return self.blobref_by_path(obj[self.REF], path)
        return ref

    def get_attr(self, ref, path):
        path, name = os.path.split(path)
        ref = self.blobref_by_path(ref, path)
        for attr in self.get_tree(ref):
            if name == attr[self.NAME]:
                return attr
        return None

    def get_root(self, ref):
        blob = self.get_blob(ref)
        return dict(line.split() for line in blob.split('\n'))

    def get_roots(self):
        for filename in os.listdir(self.roots_path):
            with open(os.path.join(self.roots_path, filename)) as fp:
                yield fp.read()

    def get_tree(self, ref, path=None):
        if path is not None:
            ref = self.blobref_by_path(ref, path)
        for line in self.get_blob(ref).splitlines():
            attr = dict(zip(self.ATTR_KEYS, line.split()))
            attr[self.MODE] = int(attr[self.MODE], 8)
            yield attr

    def ignore_file(self, name):
        if not self.__ignore_patterns:
            self.__ignore_patterns = ['.btfu']
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
            attr = {
                self.MODE: st.st_mode,
                self.TYPE: (self.TYPE_BLOB if os.path.isfile(path) or
                            os.path.islink(path) else self.TYPE_TREE),
                self.REF: self.put_file(path),
                self.NAME: name,
            }
            ls.append(self.attr_to_str(attr))
        return '\n'.join(ls)

    def set_attr(self, rootref, path, new_attr):
        dirpath, filename = os.path.split(path)
        if not filename:
            return new_attr[self.REF]
        exists = False
        tree = []
        for attr in self.get_tree(rootref, dirpath):
            if attr[self.NAME] == filename:
                if self.REF in new_attr:
                    tree.append(new_attr)
                exists = True
            else:
                tree.append(attr)
        if not exists:
            tree.append(new_attr)
        tree.sort(lambda a, b: cmp(a[self.NAME], b[self.NAME]))
        ref = self.put_blob('\n'.join(self.attr_to_str(attr) for attr in tree))
        if dirpath == os.sep:
            return ref
        attr = get_attr(rootref, dirpath)
        attr[self.REF] = ref
        return set_attr(rootref, dirpath, attr)

    def files_by_path(self, ref, path):
        return [row[self.NAME] for row in self.get_tree(ref, path)]
