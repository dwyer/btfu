import BaseHTTPServer
import SocketServer


class BlobRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            ref = self.path[1:]
            blob = self.server.store.get_blob(ref)
            if blob is not None:
                self.response(blob, content_type='application/octet-stream')
            else:
                self.response(status=404)
        except Exception, e:
            self.response(str(e), status=500)

    def do_POST(self):
        try:
            if self.path == '/':
                n = int(self.headers['Content-Length'])
                self.response(self.server.store.put_blob(self.rfile.read(n)))
            else:
                self.response(status=404)
        except Exception, e:
            self.response(str(e), status=500)

    def response(self, blob='', status=200, content_type='text/plain'):
        self.send_response(status)
        self.send_header('Content-type', content_type)
        self.end_headers()
        self.wfile.write(blob)


class BlobServer(SocketServer.TCPServer):

    def __init__(self, store, host, port):
        SocketServer.TCPServer.__init__(self, (host, port), BlobRequestHandler)
        self.store = store


def serve(store, host, port):
    print 'listening on %s:%d' % (host, port)
    httpd = BlobServer(store, host, port)
    httpd.serve_forever()
