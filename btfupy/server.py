import BaseHTTPServer
import SocketServer
import traceback


class BlobRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self, head=False):
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

    def __init__(self, store, host, port):
        SocketServer.TCPServer.__init__(self, (host, port), BlobRequestHandler)
        self.store = store


def serve(store, host, port):
    print 'listening on %s:%d' % (host, port)
    httpd = BlobServer(store, host, port)
    httpd.serve_forever()
