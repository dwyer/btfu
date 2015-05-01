import BaseHTTPServer
import SocketServer
import traceback


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
            blob = self.server.store.get_blob(self.path[1:])
            if blob is not None:
                self.response(blob if not head else '',
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
                self.response(self.server.store.put_blob(self.rfile.read(n)))
            else:
                self.response(status=405)
        except:
            self.response(status=500)
            traceback.print_exc()

    def response(self, blob='', status=200, content_type='text/plain'):
        self.send_response(status)
        self.send_header('Content-type', content_type)
        self.end_headers()
        self.wfile.write(blob)


class BlobServer(SocketServer.TCPServer):

    def __init__(self, store, host, port, auth_token=None):
        SocketServer.TCPServer.__init__(self, (host, port), BlobRequestHandler)
        self.store = store
        self.auth_token = auth_token
