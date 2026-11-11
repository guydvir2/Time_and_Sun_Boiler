"""Operating the boiler needs no password; admin actions still do."""
import http.client
import json
import threading
import unittest
from datetime import datetime

from boiler.web import make_server
from tests.test_engine_safety import Base, FakeDT


class OpenControlsTest(Base):
    def setUp(self):
        super().setUp()
        FakeDT.cur = datetime.now()
        self.s.dashboard_host, self.s.dashboard_port, self.s.dashboard_password = "127.0.0.1", 0, "secret-pw-1"
        self.srv = make_server(self.s, self.drivers)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def tearDown(self):
        self.srv.shutdown(); self.srv.server_close()
        super().tearDown()

    def post(self, path, body=None, csrf=True):
        c = http.client.HTTPConnection("127.0.0.1", self.srv.server_address[1], timeout=10)
        h = {"Content-Type": "application/json"}
        if csrf:
            h["X-Requested-With"] = "boiler-ui"
        c.request("POST", path, json.dumps(body or {}), h)
        r = c.getresponse()
        return r.status, json.loads(r.read() or b"{}")

    def test_run_without_password(self):
        self.assertEqual(self.post("/api/run", {"minutes": 20})[0], 200)

    def test_run_still_needs_csrf_header(self):
        self.assertEqual(self.post("/api/run", {"minutes": 20}, csrf=False)[0], 403)

    def test_run_is_still_capped(self):
        self.assertEqual(self.post("/api/run", {"minutes": 9999})[0], 400)

    def test_missed_and_cancel_open(self):
        self.assertEqual(self.post("/api/later/cancel")[0], 200)
        self.assertEqual(self.post("/api/missed", {"id": "x", "action": "skip"})[0], 409)   # "already handled": reached the handler

    def test_admin_still_locked(self):
        for path in ("/api/settings", "/api/schedule", "/api/formula", "/api/auto"):
            self.assertEqual(self.post(path, {})[0], 401, path)


if __name__ == "__main__":
    unittest.main()
