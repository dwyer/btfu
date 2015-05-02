import urllib2
import sys

import memcache


class BlobClient(object):

    def __init__(self, baseurl, auth_token):
        self.baseurl = baseurl
        self.auth_token = auth_token
        self.memcache = memcache.Client(['127.0.0.1'])

    def get_blob(self, ref, size=-1, offset=0):
        blob = self.memcache.get(ref)
        if blob is None:
            blob = BlobRequest.get_blob(self.baseurl, ref, self.auth_token)
        if blob is not None:
            self.memcache.set(ref, blob)
            if offset > 0:
                blob = blob[offset:]
            if size > -1:
                blob = blob[:size]
        return blob

    def get_size(self, ref):
        size = self.memcache.get('size:' + ref)
        if size is not None:
            return size
        blob = self.memcache.get(ref)
        if blob is not None:
            size = len(blob)
        else:
            size = BlobRequest.get_size(self.baseurl, ref, self.auth_token)
        if size is not None:
            self.memcache.set('size:' + ref, size)
        return size

    def put_blob(self, blob):
        ref = BlobRequest.put_blob(self.baseurl, blob, self.auth_token)
        if ref is not None:
            self.memcache.set(ref, blob)
        return ref


class BlobRequest(urllib2.Request):

    def __init__(self, url, blob=None, auth_token=None):
        urllib2.Request.__init__(self, url, blob)
        if blob is not None:
            self.add_header('Content-Length', str(len(blob)))
            self.add_header('Content-Type', 'application/octet-stream')
        if auth_token is not None:
            self.add_header('Cookie', 'auth=%s' % auth_token)

    def send(self):
        try:
            response = urllib2.urlopen(self)
        except urllib2.HTTPError, e:
            print >>sys.stderr, e
            return e
        return response

    @classmethod
    def get_blob(cls, baseurl, ref, auth_token):
        request = cls('%s/%s' % (baseurl, ref), auth_token=auth_token)
        response = request.send()
        if isinstance(response, urllib2.HTTPError):
            return None
        blob = response.read()
        response.close()
        return blob

    @classmethod
    def get_size(cls, baseurl, ref, auth_token):
        request = cls('%s/%s' % (baseurl, ref), auth_token=auth_token)
        request.get_method = lambda: 'HEAD'
        response = request.send()
        if isinstance(response, urllib2.HTTPError):
            return None
        size = int(response.info().getheader('Content-Length'))
        response.close()
        return size

    @classmethod
    def has_blob(cls, baseurl, ref, auth_token):
        request = cls('%s/%s' % (baseurl, ref), auth_token=auth_token)
        request.get_method = lambda: 'HEAD'
        response = request.send()
        if isinstance(response, urllib2.HTTPError):
            if response.code == 404:
                return False
            return None
        response.close()
        return True

    @classmethod
    def put_blob(cls, baseurl, blob, auth_token):
        request = cls(baseurl, blob, auth_token=auth_token)
        response = request.send()
        if isinstance(response, urllib2.HTTPError):
            return None
        ref = response.read()
        response.close()
        return ref
