"""Host updater service: token check, branch validation, one update at a time. deploy.sh is never really run."""
import http.client
import importlib.util
import json
import os
import threading
import time

spec = importlib.util.spec_from_file_location("boiler_updater", os.path.join(os.path.dirname(__file__), "..", "updater", "boiler_updater.py"))
bu = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bu)


def test_branch_validation():
    for bad in ("", "-rf", "a b", "x;rm", "../etc", "a..b", "$(id)", None, 5):
        assert bu.start_update(bad, run=lambda b, l: 0)[0] == 400
    bu.STATE.update(state="idle")


def test_one_update_at_a_time_and_result(tmp_path, monkeypatch):
    monkeypatch.setattr(bu, "LOG_FILE", str(tmp_path / "u.log"))
    gate = threading.Event()
    bu.STATE.update(state="idle")
    code, _ = bu.start_update("feature/x", run=lambda b, log: gate.wait(5) and 0)
    assert code == 202
    assert bu.start_update("feature/y", run=lambda b, log: 0)[0] == 409       # busy
    gate.set()
    for _ in range(50):
        if bu.STATE["state"] != "running":
            break
        time.sleep(0.05)
    assert bu.STATE["state"] == "done" and bu.STATE["branch"] == "feature/x"


def test_http_requires_token(tmp_path, monkeypatch):
    monkeypatch.setattr(bu, "LOG_FILE", str(tmp_path / "u.log"))
    bu.STATE.update(state="idle")
    bu.Handler.token = "secret"
    srv = bu.ThreadingHTTPServer(("127.0.0.1", 0), bu.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]

    def call(method, path, token=None, body=None):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        c.request(method, path, json.dumps(body) if body else None, {"X-Updater-Token": token} if token else {})
        r = c.getresponse()
        return r.status, json.loads(r.read())

    assert call("GET", "/status")[0] == 401
    assert call("GET", "/status", "wrong")[0] == 401
    assert call("POST", "/update", "wrong", {"branch": "master"})[0] == 401
    assert call("POST", "/update", "secret", {"branch": "bad branch"})[0] == 400
    st, body = call("GET", "/status", "secret")
    assert st == 200 and body["state"] == "idle"
    srv.shutdown()
