#!/usr/bin/env python3
"""Run S3 exact-GET regressions through the AWS SDK HTTP stack.

Usage: python3 s3_http_test.py /path/to/myobjstore_s3_conditional_test
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import subprocess
import sys
import threading
from urllib.parse import urlsplit


class Handler(BaseHTTPRequestHandler):
    retry_requests = 0

    def do_GET(self):
        key = urlsplit(self.path).path.rsplit("/", 1)[-1]
        status, code = {
            "missing-key": (404, "NoSuchKey"),
            "missing-bucket": (404, "NoSuchBucket"),
            "forbidden": (403, "AccessDenied"),
        }.get(key, (200, ""))
        if key == "retry":
            Handler.retry_requests += 1
            if Handler.retry_requests % 2 == 1:
                status, code = 503, "SlowDown"
        if code:
            body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" +
                    code + "</Code><Message>" + "error response " * 30 +
                    "</Message><RequestId>test-request</RequestId></Error>").encode()
        else:
            body = b"x" if key in ("one-byte", "retry") else b"xx"
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "application/xml" if code else "application/octet-stream")
        self.send_header("ETag", '"test-etag"')
        self.end_headers()
        self.wfile.write(body)


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    env = dict(os.environ, WESQL_S3_TEST_ENDPOINT="http://127.0.0.1:" + str(server.server_port))
    result = subprocess.run([sys.argv[1]], env=env, timeout=90)
    if result.returncode == 0 and Handler.retry_requests != 4:
        raise RuntimeError("expected a failed and successful HTTP attempt per API")
finally:
    server.shutdown()
    server.server_close()
    thread.join()
sys.exit(result.returncode)
