import urllib2
import sys


class BlobClient:

    def __init__(self, baseurl, auth_token=None):
        self.baseurl = baseurl
        self.auth_token = auth_token

    def get_blob(self, ref):
        request = urllib2.Request('%s/%s' % (self.baseurl, ref))
        if self.auth_token is not None:
            request.add_header('Cookie', 'auth=%s' % self.auth_token)
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, e:
            print >>sys.stderr, e
            return None
        blob = response.read()
        response.close()
        return blob

    def put_blob(self, blob):
        request = urllib2.Request(self.baseurl, blob)
        request.add_header('Content-Length', str(len(blob)))
        request.add_header('Content-Type', 'application/octet-stream')
        if self.auth_token is not None:
            request.add_header('Cookie', 'auth=%s' % self.auth_token)
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, e:
            print >>sys.stderr, e
            return None
        ref = response.read()
        response.close()
        return ref
