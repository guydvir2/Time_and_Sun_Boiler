"""
core.py — domain logic.
  • WeatherService — Open-Meteo API + duration calculation
  • DataManager    — daily CSV log
"""
import csv
import logging
import os
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils import with_retries

log = logging.getLogger(__name__)
_csv_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────
# Weather + solar calculation
# ─────────────────────────────────────────────────────────────

class WeatherService:
    def __init__(self, weather_url: str, lat: float, lon: float,
                 temp_lut: Dict[int, int],
                 cloud_penalty_factor: float,
                 max_first_run: int):
        self.weather_url          = weather_url
        self.lat                  = lat
        self.lon                  = lon
        self.temp_lut             = temp_lut
        self.cloud_penalty_factor = cloud_penalty_factor
        self.max_first_run        = max_first_run

    # ── Sun times ────────────────────────────────────────────

    def get_sun_times(self) -> Tuple[datetime, datetime]:
        def _fetch():
            r = requests.get(self.weather_url, params={
                "latitude": self.lat, "longitude": self.lon,
                "daily": "sunrise,sunset",
                "timezone": "auto", "forecast_days": 1,
            }, timeout=10)
            r.raise_for_status()
            return r.json()

        try:
            data = with_retries(_fetch, label="sun times fetch")
        except Exception as e:
            raise RuntimeError(f"Could not fetch sun times: {e}") from e

        try:
            sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0])
            sunset  = datetime.fromisoformat(data["daily"]["sunset"][0])
        except (KeyError, IndexError, ValueError) as e:
            raise RuntimeError(f"Unexpected API response: {e}") from e

        log.info(f"Sun: ↑{sunrise.strftime('%H:%M')} ↓{sunset.strftime('%H:%M')}")
        return sunrise, sunset

    # ── Measured weather ─────────────────────────────────────

    def get_measured_weather(self, sunrise: datetime, sunset: datetime) -> List[Dict]:
        def _fetch():
            r = requests.get(self.weather_url, params={
                "latitude": self.lat, "longitude": self.lon,
                "hourly": "temperature_2m,cloudcover",
                "timezone": "auto",
            }, timeout=10)
            r.raise_for_status()
            return r.json()

        try:
            data = with_retries(_fetch, label="hourly weather fetch")
        except requests.exceptions.Timeout:
            raise RuntimeError("Weather API timed out") from None
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Weather API failed: {e}") from e

        today    = date.today()
        now      = datetime.now()
        end_time = min(now, sunset)
        result   = []

        for t, temp, cloud in zip(
                data["hourly"]["time"],
                data["hourly"]["temperature_2m"],
                data["hourly"]["cloudcover"]):
            try:
                ts = datetime.fromisoformat(t)
            except ValueError:
                continue
            if ts.date() == today and sunrise <= ts <= end_time:
                result.append({"hour": ts, "temp": temp, "cloud": cloud})

        log.info(f"Collected {len(result)} hourly measurements "
                 f"(sunrise→{end_time.strftime('%H:%M')})")
        return result

    # ── Calculations ─────────────────────────────────────────

    def weighted_average(self, weather: List[Dict],
                         sunrise: datetime, sunset: datetime) -> Tuple[float, float]:
        total_w = temp_sum = cloud_sum = 0.0
        for w in weather:
            t = w["hour"]
            if   t < sunrise + timedelta(hours=3):          weight = 0.5
            elif t > sunset  - timedelta(hours=3):          weight = 0.5
            elif sunrise + timedelta(hours=3) <= t <= sunset - timedelta(hours=3):
                                                             weight = 1.0
            else:                                            weight = 0.0
            if weight:
                temp_sum  += w["temp"]  * weight
                cloud_sum += w["cloud"] * weight
                total_w   += weight
        if not total_w:
            raise ValueError("No measured weather data for weighting")
        return temp_sum / total_w, cloud_sum / total_w

    def calculate_duration(self, temp: float) -> int:
        temps = sorted(self.temp_lut.keys())
        if temp <= temps[0]:  return self.temp_lut[temps[0]]
        if temp >= temps[-1]: return self.temp_lut[temps[-1]]
        for i in range(len(temps) - 1):
            t0, t1 = temps[i], temps[i + 1]
            if t0 <= temp <= t1:
                v0, v1 = self.temp_lut[t0], self.temp_lut[t1]
                return int(round(v0 + (temp - t0) / (t1 - t0) * (v1 - v0)))
        raise ValueError(f"Temperature {temp}°C not matched in LUT")

    def calculate_sliders(self, duration: int) -> Tuple[int, int]:
        if duration <= self.max_first_run:
            return duration, 0
        return self.max_first_run, duration - self.max_first_run

    def calculate_all(self, sunrise: datetime, sunset: datetime) -> Dict:
        weather                  = self.get_measured_weather(sunrise, sunset)
        avg_temp, avg_cloud      = self.weighted_average(weather, sunrise, sunset)
        effective_temp           = round(
            avg_temp - (avg_cloud / 100) * self.cloud_penalty_factor, 2)
        duration                 = self.calculate_duration(effective_temp)
        first_run, second_run    = self.calculate_sliders(duration)
        log.info(f"Calc: avg={avg_temp:.1f}°C cloud={avg_cloud:.0f}% "
                 f"eff={effective_temp}°C dur={duration}min "
                 f"sliders=({first_run},{second_run})")
        return {
            "weather_data":   weather,
            "avg_temp":       round(avg_temp, 2),
            "avg_cloud":      round(avg_cloud, 2),
            "effective_temp": effective_temp,
            "duration":       duration,
            "first_run":      first_run,
            "second_run":     second_run,
        }


# ─────────────────────────────────────────────────────────────
# Daily CSV log
# ─────────────────────────────────────────────────────────────

def _safe_num(value, cast=float, fallback=None):
    try:
        return cast(value) if value not in (None, "", "—", "N/A") else fallback
    except (ValueError, TypeError):
        return fallback


class DataManager:
    # manual_minutes removed — tracked in scheduler memory only
    COLUMNS = [
        "date", "dawn", "dusk", "avg_temp", "avg_cloud", "effective_temp",
        "duration", "first_run", "second_run", "trigger_time", "ha_status",
    ]

    def __init__(self, csv_file: Optional[str] = None):
        from config import DataDirectoryManager
        self.DAILY_LOG_FILE = csv_file or DataDirectoryManager.get_csv_path()
        self._cache: Optional[List] = None
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.DAILY_LOG_FILE):
            with _csv_lock:
                with open(self.DAILY_LOG_FILE, "w", newline="") as f:
                    csv.writer(f).writerow(self.COLUMNS)
            log.info(f"Created {self.DAILY_LOG_FILE}")

    def invalidate(self):
        self._cache = None

    # ── Read ─────────────────────────────────────────────────

    def _read_all_rows(self) -> List[Dict[str, str]]:
        if not os.path.exists(self.DAILY_LOG_FILE):
            return []
        with open(self.DAILY_LOG_FILE, newline="") as f:
            return list(csv.DictReader(f))

    def all_records(self) -> List[Dict[str, Any]]:
        if self._cache is not None:
            return self._cache
        rows = self._read_all_rows()
        try:
            rows.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))
        except ValueError as e:
            log.warning(f"Sort by date failed: {e}")
        self._cache = rows
        return self._cache

    def today_record(self) -> Optional[Dict[str, str]]:
        today = str(date.today())
        return next((r for r in self._read_all_rows() if r.get("date") == today), None)

    # ── Write ────────────────────────────────────────────────

    def save_record(self, row: Dict[str, Any]) -> bool:
        try:
            with _csv_lock:
                rows = self._read_all_rows()
                rows = [r for r in rows if r.get("date") != row["date"]]
                rows.append(row)
                self._write_all_rows(rows)
            log.info(f"Saved record for {row['date']}")
            self.invalidate()
            return True
        except Exception as e:
            log.error(f"Failed to save record: {e}")
            return False

    def _write_all_rows(self, rows: List[Dict]):
        tmp = self.DAILY_LOG_FILE + ".tmp"
        with open(tmp, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.COLUMNS)
            w.writeheader()
            for row in rows:
                w.writerow({c: row.get(c, "") for c in self.COLUMNS})
        os.replace(tmp, self.DAILY_LOG_FILE)

    # ── Retry ────────────────────────────────────────────────

    def retry_ha(self, ha_service) -> Tuple[bool, str]:
        today_str = str(date.today())
        today_row = self.today_record()
        if not today_row:
            log.warning("No record for today — cannot retry HA")
            return False, "No data for today"

        first_run = _safe_num(today_row.get("first_run"), cast=int, fallback=0)
        ha_ok     = ha_service.send_run(first_run, run_number=1)
        today_row["ha_status"]    = "OK" if ha_ok else "FAILED"
        today_row["trigger_time"] = datetime.now().strftime("%H:%M:%S")

        with _csv_lock:
            rows = self._read_all_rows()
            rows = [today_row if r.get("date") == today_str else r for r in rows]
            self._write_all_rows(rows)

        log.info(f"Retry HA: {'OK' if ha_ok else 'FAILED'}")
        self.invalidate()
        return ha_ok, today_row["ha_status"]
