import distutils.dir_util
import hashlib
import os
import stat
import sys


class BlobStore(object):

    def __init__(self, path):
        self.store_path = path
        self.blobs_path = os.path.join(self.store_path, 'blobs')
        self.roots_path = os.path.join(self.store_path, 'roots')
        for path in [self.store_path, self.blobs_path, self.roots_path]:
            if not os.path.exists(path):
                try:
                    os.mkdir(path)
                except OSError, e:
                    print >>sys.stderr, e

    def __get_path(self, ref, split=False):
        try:
            a, b = ref.split('-', 1)
        except ValueError:
            return None
        return os.path.join(self.blobs_path, a, b[0:2], b[2:4], b[4:])

    def blobref(self, blob):
        return 'sha1-%s' % hashlib.sha1(blob).hexdigest()

    def get_blob(self, ref, size=-1, offset=0):
        path = self.__get_path(ref)
        if path is None or not os.path.exists(path) or os.path.isdir(path):
            return None
        try:
            with open(path, 'rb') as f:
                f.seek(offset)
                return f.read(size)
        except IOError:
            return None

    def get_size(self, ref):
        path = self.__get_path(ref)
        if path is None or not os.path.exists(path) or os.path.isdir(path):
            return None
        return os.stat(path).st_size

    def put_blob(self, blob):
        ref = self.blobref(blob)
        path = self.__get_path(ref)
        if not os.path.exists(path):
            distutils.dir_util.mkpath(os.path.dirname(path))
            with open(path, 'wb') as f:
                f.write(blob)
            os.chmod(path, 0400)
        return ref
