"""
Weather Service
Handles weather API calls and duration calculations
NO verbose logging - only essentials
"""

import logging
import requests
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple
from ha_service import with_retries

log = logging.getLogger(__name__)


class WeatherService:
    """Weather API client and calculation engine"""
    
    def __init__(self, weather_url, lat, lon, temp_lut, cloud_penalty_factor, max_first_run):
        self.weather_url = weather_url
        self.lat = lat
        self.lon = lon
        self.temp_lut = temp_lut
        self.cloud_penalty_factor = cloud_penalty_factor
        self.max_first_run = max_first_run
    
    def get_sun_times(self) -> Tuple[datetime, datetime]:
        """Fetch sunrise and sunset times"""
        def _fetch():
            r = requests.get(
                self.weather_url,
                params={
                    "latitude": self.lat,
                    "longitude": self.lon,
                    "daily": "sunrise,sunset",
                    "timezone": "auto",
                    "forecast_days": 1
                },
                timeout=10
            )
            r.raise_for_status()
            return r.json()
        
        try:
            data = with_retries(_fetch, label="sun times fetch")
        except Exception as e:
            raise RuntimeError(f"Could not fetch sun times: {e}") from e
        
        try:
            sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0])
            sunset = datetime.fromisoformat(data["daily"]["sunset"][0])
        except (KeyError, IndexError, ValueError) as e:
            raise RuntimeError(f"Unexpected API response for sun times: {e}") from e
        
        log.info(f"Sun times: sunrise={sunrise.strftime('%H:%M')}, sunset={sunset.strftime('%H:%M')}")
        return sunrise, sunset
    
    def get_measured_weather(self, sunrise: datetime, sunset: datetime) -> List[Dict]:
        """
        Fetch hourly weather from sunrise to sunset (measured hours only).
        Returns: list of {"hour": datetime, "temp": float, "cloud": int}
        """
        def _fetch():
            r = requests.get(
                self.weather_url,
                params={
                    "latitude": self.lat,
                    "longitude": self.lon,
                    "hourly": "temperature_2m,cloudcover",
                    "timezone": "auto"
                },
                timeout=10
            )
            r.raise_for_status()
            return r.json()
        
        try:
            data = with_retries(_fetch, label="hourly weather fetch")
        except requests.exceptions.Timeout:
            raise RuntimeError("Weather API timed out") from None
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Weather API failed: {e}") from e
        
        today = date.today()
        now = datetime.now()
        weather = []
        
        for t, temp, clouds in zip(
            data["hourly"]["time"],
            data["hourly"]["temperature_2m"],
            data["hourly"]["cloudcover"]
        ):
            try:
                ts = datetime.fromisoformat(t)
            except ValueError:
                continue
            
            # Only measured hours: today, from sunrise to min(now, sunset)
            end_time = min(now, sunset)
            if ts.date() == today and sunrise <= ts <= end_time:
                weather.append({"hour": ts, "temp": temp, "cloud": clouds})
        
        log.info(f"Collected {len(weather)} hourly measurements (sunrise→{end_time.strftime('%H:%M')})")
        return weather
    
    def weighted_average(self, weather: List[Dict], sunrise: datetime, sunset: datetime) -> Tuple[float, float]:
        """
        Calculate weighted average of temp and clouds.
        Weights: morning (first 3h) = 0.5, peak = 1.0, evening (last 2h) = 0.5
        """
        total_weight = temp_sum = cloud_sum = 0
        
        for w in weather:
            t = w["hour"]
            if sunrise <= t < sunrise + timedelta(hours=3):
                weight = 0.5
            elif sunrise + timedelta(hours=3) <= t <= sunset - timedelta(hours=2):
                weight = 1.0
            elif sunset - timedelta(hours=2) < t <= sunset:
                weight = 0.5
            else:
                weight = 0
            
            if weight > 0:
                temp_sum += w["temp"] * weight
                cloud_sum += w["cloud"] * weight
                total_weight += weight
        
        if total_weight == 0:
            raise ValueError("No measured weather data for weighting")
        
        return temp_sum / total_weight, cloud_sum / total_weight
    
    def calculate_duration(self, temp: float) -> int:
        """Linear interpolation in LUT"""
        temps = sorted(self.temp_lut.keys())
        if temp <= temps[0]:
            return self.temp_lut[temps[0]]
        if temp >= temps[-1]:
            return self.temp_lut[temps[-1]]
        
        for i in range(len(temps) - 1):
            t0, t1 = temps[i], temps[i + 1]
            if t0 <= temp <= t1:
                v0, v1 = self.temp_lut[t0], self.temp_lut[t1]
                return int(round(v0 + (temp - t0) / (t1 - t0) * (v1 - v0)))
        
        raise ValueError(f"Temperature {temp}°C not matched in LUT")
    
    def calculate_sliders(self, duration: int) -> Tuple[int, int]:
        """Split duration into first_run and second_run"""
        if duration <= self.max_first_run:
            return duration, 0
        return self.max_first_run, duration - self.max_first_run
    
    def calculate_all(self, sunrise: datetime, sunset: datetime) -> Dict:
        """
        Full calculation pipeline:
        1. Fetch weather
        2. Calculate weighted averages
        3. Apply cloud penalty
        4. Calculate duration and sliders
        
        Returns dict with all calculated values
        """
        weather = self.get_measured_weather(sunrise, sunset)
        avg_temp, avg_cloud = self.weighted_average(weather, sunrise, sunset)
        effective_temp = round(avg_temp - (avg_cloud / 100) * self.cloud_penalty_factor, 2)
        duration = self.calculate_duration(effective_temp)
        first_run, second_run = self.calculate_sliders(duration)
        
        log.info(f"Calculation: avg_temp={avg_temp:.1f}°C, avg_cloud={avg_cloud:.0f}%, "
                f"effective={effective_temp}°C, duration={duration}min, "
                f"sliders=({first_run},{second_run})")
        
        return {
            "weather_data": weather,
            "avg_temp": round(avg_temp, 2),
            "avg_cloud": round(avg_cloud, 2),
            "effective_temp": effective_temp,
            "duration": duration,
            "first_run": first_run,
            "second_run": second_run
        }
