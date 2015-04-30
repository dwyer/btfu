import BaseHTTPServer
import SocketServer

import bs

store = bs.BlobStore()

class Handler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        ref = self.path[1:]
        blob = store.get_blob(ref)
        if blob is not None:
            self.response(blob, content_type='application/octet-stream')
        else:
            self.response(status=404)

    def do_POST(self):
        if self.path == '/':
            n = int(self.headers['Content-Length'])
            self.response(store.put_blob(self.rfile.read(n)))
        else:
            self.response(status=404)

    def response(self, blob='', status=200, content_type='text/plain'):
        self.send_response(status)
        self.send_header('Content-type', content_type)
        self.end_headers()
        self.wfile.write(blob)


def serve(port=bs.DEFAULT_PORT):
    httpd = SocketServer.TCPServer(('', port), Handler)
    httpd.serve_forever()
