"""
utils.py — shared low-level utilities.
  • Timezone helpers (init_timezone / now_local)
  • with_retries — used by services and weather
"""
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

_tz = None


def init_timezone(tz_string: str):
    """Call once at startup."""
    global _tz
    _tz = ZoneInfo(tz_string)


def now_local() -> datetime:
    """Current datetime in configured timezone."""
    return datetime.now(tz=_tz or ZoneInfo("Asia/Jerusalem"))


def with_retries(fn, retries: int = 3, delay: int = 5, label: str = "operation"):
    """Call fn up to `retries` times, sleeping `delay` seconds between attempts."""
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == retries:
                log.error(f"{label} failed after {retries} attempts: {e}")
                raise
            log.warning(f"{label} attempt {attempt}/{retries} failed: {e} — retry in {delay}s")
            time.sleep(delay)
