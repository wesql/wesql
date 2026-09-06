#!/usr/bin/env python3
"""Forward real S3 HTTP I/O and lose selected applied responses on demand."""

import argparse
import datetime
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import threading
import urllib.parse


MODES = {"segment_response_lost", "head_response_lost",
         "head_response_unreconciled"}


class FaultProxy(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, upstream, evidence):
        super().__init__(address, Forward)
        self.upstream = upstream
        self.evidence = Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.guard = threading.Lock()
        self.mode = None
        self.consumed = False
        self.blocked = False

    def record(self, event):
        event["time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self.guard:
            with (self.evidence / "http.jsonl").open("a") as output:
                output.write(json.dumps(event, sort_keys=True) + "\n")
                output.flush()
                os.fsync(output.fileno())

    def arm(self, mode):
        if mode not in MODES:
            raise ValueError("unknown fault mode")
        with self.guard:
            if self.mode is not None or self.consumed:
                raise RuntimeError("proxy fault is one-shot")
            self.mode = mode
        self.record({"event": "armed", "mode": mode})

    def refresh_arm(self):
        path = self.evidence / "arm.json"
        with self.guard:
            if self.mode is not None or self.consumed or not path.exists():
                return
            path.rename(self.evidence / "arm.claimed.json")
            mode = json.loads((self.evidence / "arm.claimed.json").read_text())["mode"]
            if mode not in MODES:
                raise ValueError("unknown fault mode")
            self.mode = mode

    def lose_response(self, method, path, status):
        with self.guard:
            if self.consumed or self.mode is None or method != "PUT" or not 200 <= status < 300:
                return False
            key = urllib.parse.urlsplit(path).path
            matches = ("/binlog/segments/" in key if self.mode == "segment_response_lost"
                       else key.endswith("/HEAD"))
            if not matches:
                return False
            self.consumed = True
            self.blocked = self.mode == "head_response_unreconciled"
            return True


class Forward(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def drop(self):
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.connection.close()

    def request_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                line = self.rfile.readline(65536)
                size = int(line.split(b";", 1)[0], 16)
                chunks.append(line)
                if size == 0:
                    while True:
                        trailer = self.rfile.readline(65536)
                        chunks.append(trailer)
                        if trailer == b"\r\n":
                            return b"".join(chunks)
                        if not trailer:
                            raise ValueError("truncated chunk trailers")
                body = self.rfile.read(size + 2)
                if len(body) != size + 2 or body[-2:] != b"\r\n":
                    raise ValueError("truncated chunk")
                chunks.append(body)
        size = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(size)
        if len(body) != size:
            raise ValueError("truncated request body")
        return body

    def forward(self):
        event = {"method": self.command, "path": self.path,
                 "if_match": self.headers.get("If-Match"),
                 "if_none_match": self.headers.get("If-None-Match")}
        upstream = None
        try:
            self.server.refresh_arm()
            if self.server.blocked:
                self.server.record(dict(event, event="blocked_before_upstream"))
                self.drop()
                return
            body = self.request_body()
            event.update(request_bytes=len(body), request_sha256=hashlib.sha256(body).hexdigest())
            upstream = http.client.HTTPConnection(*self.server.upstream, timeout=60)
            upstream.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for key, value in self.headers.items():
                if key.lower() not in {"connection", "proxy-connection"}:
                    upstream.putheader(key, value)
            upstream.putheader("Connection", "close")
            upstream.endheaders(body)
            response = upstream.getresponse()
            event.update(upstream_status=response.status, etag=response.getheader("ETag"))
            lose = self.server.lose_response(self.command, self.path, response.status)
            if not lose:
                self.send_response_only(response.status, response.reason)
                for key, value in response.getheaders():
                    if key.lower() not in {"connection", "transfer-encoding"}:
                        self.send_header(key, value)
                self.send_header("Connection", "close")
                self.end_headers()
            sha = hashlib.sha256()
            size = 0
            while True:
                data = response.read(65536)
                if not data:
                    break
                sha.update(data)
                size += len(data)
                if not lose:
                    self.wfile.write(data)
            event.update(response_bytes=size, response_sha256=sha.hexdigest(),
                         event="applied_response_dropped" if lose else "forwarded")
            self.server.record(event)
            if lose:
                self.drop()
            self.close_connection = True
        except Exception as error:
            self.server.record(dict(event, event="proxy_error", error=str(error)))
            self.drop()
        finally:
            if upstream is not None:
                upstream.close()

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream-port", type=int, required=True)
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=0)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    server = FaultProxy((args.listen_host, args.listen_port),
                        (args.upstream_host, args.upstream_port), args.evidence)
    print(json.dumps({"listen": server.server_address}), flush=True)
    server.serve_forever()
