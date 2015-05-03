import distutils.dir_util
import hashlib
import os
import stat
import sys
import uuid

from . import abstract


class LocalBlobStore(abstract.BlobStore):

    def __init__(self, path):
        self.store_path = path
        self.blobs_path = os.path.join(self.store_path, 'blobs')
        self.links_path = os.path.join(self.store_path, 'links')
        for path in [self.store_path, self.blobs_path, self.links_path]:
            if not os.path.exists(path):
                try:
                    os.mkdir(path)
                except OSError, e:
                    print >>sys.stderr, e

    def __get_blob_path(self, ref, split=False):
        try:
            a, b = ref.split('-', 1)
        except ValueError:
            return None
        return os.path.join(self.blobs_path, a, b[0:2], b[2:4], b[4:])

    def __get_link_path(self, link):
        return os.path.join(self.links_path, link)

    def blobref(self, blob):
        return 'sha1-%s' % hashlib.sha1(blob).hexdigest()

    def get_blob(self, ref, size=-1, offset=0):
        path = self.__get_blob_path(ref)
        if path is None or not os.path.exists(path) or os.path.isdir(path):
            return None
        try:
            with open(path, 'rb') as f:
                f.seek(offset)
                return f.read(size)
        except IOError:
            return None

    def get_link(self, link):
        if not link:
            return '\n'.join(sorted(os.listdir(self.links_path)))
        try:
            with open(self.__get_link_path(link)) as fp:
                return fp.read()
        except IOError:
            return None

    def get_size(self, ref):
        path = self.__get_blob_path(ref)
        if path is None or not os.path.exists(path) or os.path.isdir(path):
            return None
        return os.stat(path).st_size

    def has_blob(self, ref):
        return os.path.isfile(self.__get_blob_path(ref))

    def put_blob(self, blob):
        ref = self.blobref(blob)
        path = self.__get_blob_path(ref)
        if not os.path.exists(path):
            distutils.dir_util.mkpath(os.path.dirname(path))
            with open(path, 'wb') as f:
                f.write(blob)
            os.chmod(path, 0400)
        return ref

    def set_link(self, link, ref):
        if not link and not ref:
            return None
        if not link:
            link = 'uuid4-%s' % str(uuid.uuid4())
        path = self.__get_link_path(link)
        if not ref:
            if not os.path.isfile(path):
                return None
            os.remove(path)
            return link
        if os.path.isdir(path):
            return None
        try:
            with open(path, 'w') as fp:
                fp.write(ref)
        except IOError:
            return None
        return link
