import BaseHTTPServer
import SocketServer
import os
import ssl
import traceback

from . import local

BLOBS_PATH = '/blobs/'
LINKS_PATH = '/links/'


class BlobRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def authenticate(self):
        if self.server.auth_token is None:
            return True
        return self.server.auth_token == self.headers.get('Authorization')

    def do_DELETE(self):
        if not self.authenticate():
            self.send_error(403)
            return
        length = int(self.headers['Content-Length'])
        content = self.rfile.read(length)
        if self.path.startswith(LINKS_PATH):
            link = self.path[len(LINKS_PATH):]
            self.send_content(self.server.set_link(link, None))
        else:
            self.send_error(501)

    def do_GET(self):
        if not self.authenticate():
            self.send_error(403)
            return
        if self.path.startswith(BLOBS_PATH):
            blob = self.server.get_blob(self.path[len(BLOBS_PATH):])
            if blob is not None:
                self.send_content(blob)
            else:
                self.send_error(404)
        elif self.path.startswith(LINKS_PATH):
            blobref = self.server.get_link(self.path[len(LINKS_PATH):])
            if blobref is not None:
                self.send_content(blobref)
            else:
                self.send_error(404)
        else:
            self.send_error(501)

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        if not self.authenticate():
            self.send_error(403)
            return
        length = int(self.headers['Content-Length'])
        content = self.rfile.read(length)
        if self.path == BLOBS_PATH:
            self.send_content(self.server.put_blob(content))
        elif self.path == LINKS_PATH:
            self.send_content(self.server.set_link(None, content))
        else:
            self.send_error(501)

    def do_PUT(self):
        if not self.authenticate():
            self.send_error(403)
            return
        length = int(self.headers['Content-Length'])
        content = self.rfile.read(length)
        if self.path.startswith(LINKS_PATH):
            link = self.path[len(LINKS_PATH):]
            self.send_content(self.server.set_link(link, content))
        else:
            self.send_error(501)

    def handle_one_request(self):
        self.protocol_version = 'HTTP/1.1'
        try:
            BaseHTTPServer.BaseHTTPRequestHandler.handle_one_request(self)
        except:
            self.send_error(500)
            traceback.print_exc()

    def send_content(self, content, content_type='text/plain', code=200):
        self.send_response(code)
        self.send_header('Content-Length', str(len(content)))
        self.send_header('Content-Type', content_type)
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(content)

    def send_error(self, code, message=None):
        self.send_content('', code=code)


class BlobServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer,
                 local.LocalBlobStore):

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
