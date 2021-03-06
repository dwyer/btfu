import hashlib
import hmac
import httplib
import socket
import ssl
import sys
import time
import urlparse
import wsgiref.handlers

from . import cache
from . import server


class BlobClient(cache.BlobCache):

    def __init__(self, baseurl, auth_token=None, cache_path=None,
                 memcache_url=None):
        super(BlobClient, self).__init__(cache_path, memcache_url=memcache_url)
        self.baseurl = baseurl
        self.auth_token = auth_token
        url = urlparse.urlparse(baseurl)
        if url.scheme == 'http':
            self.connection = httplib.HTTPConnection(url.hostname, url.port)
        elif url.scheme == 'https':
            try:
                self.connection = httplib.HTTPSConnection(
                    url.hostname, url.port,
                    context=ssl.SSLContext(ssl.PROTOCOL_TLSv1_2))
            except AttributeError:
                self.connection = httplib.HTTPSConnection(
                    url.hostname, url.port)
        else:
            raise ValueError('invalid URL scheme: %r' % url.scheme)

    def __request(self, method, path, data=None):
        self.connection.putrequest(method, path)
        if data is not None:
            self.connection.putheader('Content-Length', str(len(data)))
            self.connection.putheader('Content-Type',
                                      'application/octet-stream')
        else:
            self.connection.putheader('Content-Length', '0')
        date = wsgiref.handlers.format_date_time(time.time())
        self.connection.putheader('Date', date)
        if self.auth_token:
            signature = hmac.new(self.auth_token, digestmod=hashlib.sha1)
            signature.update(method)
            signature.update(path)
            signature.update(date)
            self.connection.putheader('Authorization', signature.hexdigest())
        try:
            self.connection.endheaders()
        except socket.error, e:
            print >>sys.stderr, 'FATAL: %s: %s' % (self.baseurl, e)
            exit(e[0])
        if data is not None:
            self.connection.send(data)
        response = self.connection.getresponse()
        if response.status == httplib.FORBIDDEN:
            print >>sys.stderr, 'ERROR: Authorization failed.'
        elif response.status == httplib.INTERNAL_SERVER_ERROR:
            print >>sys.stderr, 'WARNING: Internal server error.'
        connection = response.getheader('Connection')
        if connection and connection.lower() == 'close':
            print >>sys.stderr, 'FATAL: Server closed the connection.'
            exit(response.status)
        return response

    def __get_blob_request(self, ref):
        response = self.__request('GET', server.BLOBS_PATH + ref)
        content = response.read()
        if response.status == httplib.OK:
            return content
        return None

    def __get_size_request(self, ref):
        response = self.__request('HEAD', server.BLOBS_PATH + ref)
        size = int(response.getheader('Content-Length'))
        content = response.read()
        if response.status == httplib.OK:
            return size
        return None

    def __has_blob_request(self, ref):
        response = self.__request('HEAD', server.BLOBS_PATH + ref)
        content = response.read()
        if response.status == httplib.OK:
            return True
        elif response.status == httplib.NOT_FOUND:
            return False
        return None

    def __put_blob_request(self, blob):
        response = self.__request('POST', server.BLOBS_PATH, blob)
        content = response.read()
        if response.status == httplib.OK:
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

    def get_link(self, link):
        if link is None:
            link = ''
        response = self.__request('GET', server.LINKS_PATH + link)
        content = response.read()
        if response.status == httplib.OK:
            return content
        return None

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

    def set_link(self, link, ref):
        if link is None:
            link = ''
        if ref:
            response = self.__request('PUT', server.LINKS_PATH + link, ref)
        else:
            response = self.__request('DELETE', server.LINKS_PATH + link)
        content = response.read()
        if response.status == httplib.OK:
            return content
        return None
