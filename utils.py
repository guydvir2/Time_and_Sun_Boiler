# utils.py
from datetime import datetime
from zoneinfo import ZoneInfo

_tz = None  # set once at startup

def init_timezone(tz_string: str):
    """Call once at app startup with config.timezone"""
    global _tz
    _tz = ZoneInfo(tz_string)

def now_local() -> datetime:
    """Current datetime in app timezone. Use everywhere instead of datetime.now()"""
    return datetime.now(tz=_tz or ZoneInfo("Asia/Jerusalem"))