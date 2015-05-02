import httplib
import os
import socket
import sys
import urllib2
import urlparse

from . import cache


class BlobClient(cache.BlobCache):

    def __init__(self, baseurl, auth_token=None, cache_path=None,
                 memcache_url=None):
        if cache_path is None:
            cache_path = os.path.join(os.environ['HOME'], '.btfu')
        super(BlobClient, self).__init__(cache_path, memcache_url=memcache_url)
        self.baseurl = baseurl
        self.auth_token = auth_token
        url = urlparse.urlparse(baseurl)
        if url.scheme == 'http':
            connection_class = httplib.HTTPConnection
        elif url.scheme == 'https':
            connection_class = httplib.HTTPSConnection
        self.connection = connection_class(url.hostname, url.port)

    def __request(self, method, ref_or_blob):
        if method == 'POST':
            blob = ref_or_blob
            path = '/'
        else:
            blob = None
            path = '/%s' % ref_or_blob
        self.connection.putrequest(method, path)
        if blob is not None:
            self.connection.putheader('Content-Length', str(len(blob)))
            self.connection.putheader('Content-Type',
                                      'application/octet-stream')
        if self.auth_token is not None:
            self.connection.putheader('Cookie', 'auth=%s' % self.auth_token)
        try:
            self.connection.endheaders()
        except socket.error, e:
            print >>sys.stderr, '%s: %s' % (self.baseurl, e)
            exit(e[0])
        if blob is not None:
            self.connection.send(blob)
        return self.connection.getresponse()

    def __get_blob_request(self, ref):
        response = self.__request('GET', ref)
        content = response.read()
        if response.status == 200:
            return content
        return None

    def __get_size_request(self, ref):
        response = self.__request('HEAD', ref)
        size = int(response.getheader('Content-Length'))
        content = response.read()
        if response.status == 200:
            return size
        return None

    def __has_blob_request(self, ref):
        response = self.__request('HEAD', ref)
        content = response.read()
        if response.status == 200:
            return True
        elif response.status == 404:
            return False
        return None

    def __put_blob_request(self, blob):
        response = self.__request('POST', blob)
        content = response.read()
        if response.status in [200, 304]:
            return content
        return None

    def get_blob(self, ref, size=-1, offset=0):
        blob = super(BlobClient, self).get_blob(ref, size=size, offset=offset)
        if blob is None:
            blob = self.__get_blob_request(ref)
        if blob is not None:
            super(BlobClient, self).put_blob(blob)
            if offset > 0:
                blob = blob[offset:]
            if size > -1:
                blob = blob[:size]
        return blob

    def get_size(self, ref):
        size = super(BlobClient, self).get_size(ref)
        if size is not None:
            return size
        size = self.__get_size_request(ref)
        if size is not None:
            super(BlobClient, self).set_size(ref, size)
        return size

    def put_blob(self, blob):
        ref = self.blobref(blob)
        if self.__has_blob_request(ref):
            return ref
        ref = self.__put_blob_request(blob)
        if ref is not None:
            super(BlobClient, self).put_blob(blob)
        return ref
