import distutils.dir_util
import hashlib
import os
import stat


class BlobStore(object):

    def __init__(self, path):
        self.store_path = path
        self.blobs_path = os.path.join(self.store_path, 'blobs')
        self.roots_path = os.path.join(self.store_path, 'roots')

    def blobref(self, blob):
        return 'sha1-%s' % hashlib.sha1(blob).hexdigest()

    def get_blob(self, ref, size=-1, offset=0):
        path = self.get_blobpath(ref)
        if path is None or not os.path.exists(path) or os.path.isdir(path):
            return None
        try:
            with open(path, 'rb') as f:
                f.seek(offset)
                return f.read(size)
        except IOError:
            return None

    def get_blobpath(self, ref, split=False):
        try:
            a, b = ref.split('-', 1)
        except ValueError:
            return None
        return os.path.join(self.blobs_path, a, b[0:2], b[2:4], b[4:])

    def get_blobsize(self, ref):
        path = self.get_blobpath(ref)
        if path is None or not os.path.exists(path) or os.path.isdir(path):
            return None
        return os.stat(path).st_size

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
            os.mkdir(self.store_path)
            os.mkdir(self.blobs_path)
            os.mkdir(self.roots_path)
        except OSError, e:
            print e
