import BaseHTTPServer
import ssl
import traceback

from . import local


class BlobRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def authenticate(self):
        if self.server.auth_token is None:
            return True
        cookie = self.headers.get('Cookie')
        if cookie is None:
            return False
        try:
            key, value = cookie.split('=', 1)
        except ValueError:
            return False
        return key == 'auth' and value == self.server.auth_token

    def do_GET(self, head=False):
        if not self.authenticate():
            self.response(status=403)
            return
        try:
            blob = self.server.get_blob(self.path[1:])
            if blob is not None:
                self.response(blob if not head else '',
                              content_length=len(blob),
                              content_type='application/octet-stream')
            else:
                self.response(status=404)
        except:
            self.response(status=500)
            traceback.print_exc()

    def do_HEAD(self):
        self.do_GET(head=True)

    def do_POST(self):
        if not self.authenticate():
            self.response(status=403)
            return
        try:
            if self.path == '/':
                n = int(self.headers['Content-Length'])
                self.response(self.server.put_blob(self.rfile.read(n)))
            else:
                self.response(status=405)
        except:
            self.response(status=500)
            traceback.print_exc()

    def response(self, blob='', status=200, content_length=None,
                 content_type='text/plain'):
        if content_length is None:
            content_length = len(blob)
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', content_length)
        self.end_headers()
        self.wfile.write(blob)


class BlobServer(local.LocalBlobStore, BaseHTTPServer.HTTPServer):

    def __init__(self, path, host, port, auth_token=None, ssl_key=None,
                 ssl_cert=None):
        local.LocalBlobStore.__init__(self, path)
        BaseHTTPServer.HTTPServer.__init__(self, (host, port),
                                           BlobRequestHandler)
        self.path = path
        self.auth_token = auth_token
        if ssl_key and ssl_cert:
            self.socket = ssl.wrap_socket(self.socket, keyfile=ssl_key,
                                          certfile=ssl_cert,
                                          cert_reqs=ssl.CERT_NONE)
