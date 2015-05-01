import urllib


class BlobClient:

    def __init__(self, baseurl):
        self.baseurl = baseurl

    def get_blob(self, ref):
        response = urllib.urlopen('%s/%s' % (self.baseurl, ref))
        if response.code != 200:
            return None
        blob = response.read()
        response.close()
        return blob

    def put_blob(self, blob):
        request = urllib.Request(self.baseurl, blob)
        request.add_header('Content-Length', str(len(blob)))
        request.add_header('Content-Type', 'application/octet-stream')
        response = urllib.urlopen(request)
        if response.code != 200:
            return None
        ref = response.read()
        response.close()
        return ref
