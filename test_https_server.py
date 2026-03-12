from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)
        fname  = ""
        for line in body.split(b"\r\n"):
            if b'filename="' in line:
                fname = line.split(b'filename="')[1].split(b'"')[0].decode()
                break
        print(f"  Received: {fname}  ({len(body)} bytes)")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok":true}')
    def log_message(self, *a): pass

print("HTTPS test server running on http://127.0.0.1:8787 ...")
HTTPServer(("127.0.0.1", 8787), Handler).serve_forever()