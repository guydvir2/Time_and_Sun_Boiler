#!/usr/bin/env python3
"""BoilerMind host updater: a tiny service on the VM that runs deploy.sh when the app asks.

Why a separate program: the app container gets NO Docker access. It can only send this service a request
(branch name + secret token). The service does nothing else, and listens only on the Docker bridge address,
so it is not reachable from your LAN.

  POST /update   {"branch": "feature/x"}   header X-Updater-Token   -> starts deploy.sh <branch>
  GET  /status                              header X-Updater-Token   -> {state, branch, log tail}
"""
import hmac
import json
import os
import re
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_DIR = os.environ.get("UPDATER_APP_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIND = os.environ.get("UPDATER_BIND", "172.17.0.1")
PORT = int(os.environ.get("UPDATER_PORT", "8099"))
LOG_FILE = os.environ.get("UPDATER_LOG", os.path.join(os.path.expanduser("~"), ".boilermind-update.log"))
BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$")      # never starts with '-', no spaces or shell characters

_lock = threading.Lock()
STATE = {"state": "idle", "branch": "", "started": None, "finished": None, "exit": None}


def read_token():
    tok = os.environ.get("UPDATER_TOKEN", "")
    if tok:
        return tok
    try:
        with open(os.path.join(APP_DIR, "boiler.env")) as f:
            for line in f:
                if line.startswith("UPDATER_TOKEN="):
                    return line.split("=", 1)[1].strip().strip('"\'')
    except OSError:
        pass
    return ""


def log_tail(n=15):
    try:
        with open(LOG_FILE, errors="replace") as f:
            lines = f.read().splitlines()[-n:]
        return [re.sub(r"\x1b\[[0-9;]*m", "", x) for x in lines]
    except OSError:
        return []


def start_update(branch, run=None):
    """Start deploy.sh for `branch`. Returns (http_code, message). `run` is injectable for tests."""
    if not isinstance(branch, str) or not BRANCH_RE.match(branch) or ".." in branch:
        return 400, "Invalid branch name"
    with _lock:
        if STATE["state"] == "running":
            return 409, "An update is already running"
        STATE.update(state="running", branch=branch, started=time.time(), finished=None, exit=None)
    threading.Thread(target=_work, args=(branch, run), daemon=True).start()
    return 202, f"Update to {branch} started"


def _work(branch, run):
    try:
        with open(LOG_FILE, "w") as log:
            log.write(f"== update to {branch} at {time.strftime('%F %T')}\n")
            log.flush()
            if run:
                code = run(branch, log)
            else:
                code = subprocess.run([os.path.join(APP_DIR, "deploy.sh"), branch], cwd=APP_DIR, stdin=subprocess.DEVNULL,
                                      stdout=log, stderr=subprocess.STDOUT).returncode
    except Exception as e:                                   # never leave the state stuck on "running"
        code = -1
        try:
            with open(LOG_FILE, "a") as log:
                log.write(f"updater error: {e}\n")
        except OSError:
            pass
    with _lock:
        STATE.update(state="done" if code == 0 else "failed", finished=time.time(), exit=code)


class Handler(BaseHTTPRequestHandler):
    token = ""

    def _send(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _authed(self):
        given = self.headers.get("X-Updater-Token", "")
        if self.token and hmac.compare_digest(given.encode(), self.token.encode()):
            return True
        self._send(401, {"ok": False, "message": "Bad token"})
        return False

    def do_GET(self):
        if self.path != "/status":
            return self._send(404, {"ok": False, "message": "Not found"})
        if self._authed():
            with _lock:
                st = dict(STATE)
            self._send(200, dict(st, ok=True, log=log_tail()))

    def do_POST(self):
        if self.path != "/update":
            return self._send(404, {"ok": False, "message": "Not found"})
        if not self._authed():
            return
        try:
            n = min(int(self.headers.get("Content-Length", "0")), 2000)
            branch = json.loads(self.rfile.read(n) or b"{}").get("branch")
        except (ValueError, AttributeError):
            return self._send(400, {"ok": False, "message": "Bad request"})
        code, msg = start_update(branch)
        self._send(code, {"ok": code == 202, "message": msg})

    def log_message(self, *args):
        pass


def main():
    Handler.token = read_token()
    if not Handler.token:
        sys.exit("UPDATER_TOKEN is empty: run updater/install.sh (it creates one in boiler.env)")
    srv = ThreadingHTTPServer((BIND, PORT), Handler)
    print(f"BoilerMind updater on {BIND}:{PORT}, app folder {APP_DIR}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
