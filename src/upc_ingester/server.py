from __future__ import annotations

import logging
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


logger = logging.getLogger(__name__)


class MirrorRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def log_message(self, format: str, *args) -> None:
        logger.info("%s - %s", self.address_string(), format % args)


def start_static_server(public_dir: Path, port: int) -> ThreadingHTTPServer:
    public_dir.mkdir(parents=True, exist_ok=True)
    handler = partial(MirrorRequestHandler, directory=str(public_dir))
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    thread = threading.Thread(target=server.serve_forever, name="static-server", daemon=True)
    thread.start()
    logger.info("serving static mirror from %s on port %s", public_dir, port)
    return server
