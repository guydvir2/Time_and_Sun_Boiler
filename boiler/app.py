"""The live configuration shared by the engine and the dashboard. When settings.json changes (Settings tab
or wizard), both pick up the new settings and drivers on their next look, no restart needed.
(Dashboard host/port are the exception: those need a restart.)"""
import os
import threading

from .settings import Settings
from .drivers import build_drivers


class App:
    def __init__(self, settings_factory=Settings, view_only=False):
        self._factory, self.view_only = settings_factory, view_only
        self._lock = threading.Lock()
        self.version = 0
        self._load()

    def _mtime(self):
        p = self.s.settings_json if hasattr(self, "s") else None
        try:
            return os.path.getmtime(p) if p else None
        except OSError:
            return None

    def _load(self):
        self.s = self._factory()
        self.drivers, self.problems = build_drivers(self.s) if not self.s.needs_setup else ([], [])
        if self.view_only:
            self.drivers = []                    # dashboard.py: never switches anything
        self.mtime = self._mtime()
        self.version += 1

    def reload(self):
        with self._lock:
            self._load()

    def maybe_reload(self):
        """Reload if settings.json changed. Returns True when it did."""
        with self._lock:
            if self._mtime() != self.mtime:
                self._load()
                return True
            return False
