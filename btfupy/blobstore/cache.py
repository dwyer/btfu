import os

import memcache

from . import local


class BlobCache(local.LocalBlobStore):

    def __init__(self, path=None, memcache_url=None):
        if path is None:
            path = os.path.join(os.environ['HOME'], '.cache', 'btfu')
        super(BlobCache, self).__init__(path)
        self.memcache_client = memcache.Client([memcache_url or '127.0.0.1'])

    @classmethod
    def __get_memcache_key(cls, prefix, postfix):
        if not prefix or not postfix:
            return None
        return 'btfu:%s:%s' % (prefix, postfix)

    def __memcache_get(self, prefix, postfix):
        if not self.memcache_client:
            return None
        key = self.__get_memcache_key(prefix, postfix)
        if key is None:
            return None
        return self.memcache_client.get(key)

    def __memcache_set(self, prefix, postfix, value):
        if not self.memcache_client:
            return
        if value is None:
            return
        key = self.__get_memcache_key(prefix, postfix)
        if key is None:
            return
        self.memcache_client.add(key, value)

    def get_blob(self, ref, size=-1, offset=0):
        blob = self.memcache_get_blob(ref)
        if blob is None:
            # Don't pass size or offset to superclass. We want the whole blob
            # so we can store it in memory.
            blob = super(BlobCache, self).get_blob(ref)
        if blob is not None:
            self.memcache_set_blob(ref, blob)
            if offset > 0:
                blob = blob[offset:]
            if size > -1:
                blob = blob[:size]
        return blob

    def get_size(self, ref):
        size = self.memcache_get_size(ref)
        if size is not None:
            return size
        size = super(BlobCache, self).get_size(ref)
        self.memcache_set_size(ref, size)
        return size

    def put_blob(self, blob):
        ref = super(BlobCache, self).put_blob(blob)
        self.memcache_set_blob(ref, blob)
        return ref

    def set_size(self, ref, size):
        self.memcache_set_size(ref, size)

    def memcache_get_blob(self, ref):
        return self.__memcache_get('blob', ref)

    def memcache_get_size(self, ref):
        return self.__memcache_get('size', ref)

    def memcache_set_blob(self, ref, blob):
        self.__memcache_set('blob', ref, blob)
        self.__memcache_set('size', ref, len(blob))

    def memcache_set_size(self, ref, size):
        self.__memcache_set('size', ref, size)
