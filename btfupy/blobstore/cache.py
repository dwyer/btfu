import memcache

from . import local


class BlobCache(local.LocalBlobStore):

    def __init__(self, path, memcache_url=None):
        super(BlobCache, self).__init__(path)
        self.memcache_client = memcache.Client([memcache_url or '127.0.0.1'])

    @classmethod
    def __get_key(cls, ref, prefix='blob'):
        return 'btfu:%s:%s' % (prefix, ref)

    def get_blob(self, ref, size=-1, offset=0):
        key = self.__get_key(ref)
        blob = self.memcache_client.get(key)
        if blob is None:
            blob = super(BlobCache, self).get_blob(ref)
        if blob is not None:
            self.memcache_client.set(key, blob)
            if offset > 0:
                blob = blob[offset:]
            if size > -1:
                blob = blob[:size]
        return blob

    def get_size(self, ref):
        size = self.memcache_client.get(self.__get_key(ref, 'size'))
        if size is not None:
            return size
        size = super(BlobCache, self).get_size(ref)
        if size is not None:
            self.set_size(ref, size)
        return size

    def put_blob(self, blob):
        ref = super(BlobCache, self).put_blob(blob)
        if ref is not None:
            self.memcache_client.set(self.__get_key(ref), blob)
        return ref

    def set_size(self, ref, size):
        self.memcache_client.set(self.__get_key(ref, 'size'), size)
