"""
boiler_model.py - physical model of a solar + electric water boiler.

Pure logic: no network, no Home Assistant, no config file access.
The app passes in weather and parameters, and gets back a day plan.

Day sequence (one fully mixed tank, single temperature T):
  1. Dawn: tank starts at T_start.
  2. Solar phase (sunrise -> sunset): collector heats the tank.
  3. Dusk: T_end_solar (calculated).
  4. Run 1: electric heating to T_max, finishing at run1_ready_by.
     Its start is calculated backwards from the energy needed.
  5. Usage: a fraction x of the tank is drawn and refilled at T_amb.
  6. Run 2: starts at run2_start, heats back to T_max.
  7. Day ends. Night standby loss gives tomorrow's T_start estimate.

Vacation mode: no electric heating at all, solar only.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import List, Optional

WH_PER_L_DEG = 1.163          # Wh to heat 1 L of water by 1 degC
J_PER_L_DEG = 4186.0          # J to heat 1 L (kg) of water by 1 degC

# Standby heat-loss coefficient by tank size (L -> UA in W/degC).
# Estimates for standard insulated tanks, not measured values.
UA_LUT = {80: 1.1, 120: 1.4, 150: 1.7, 200: 2.0, 300: 2.6}


# --------------------------------------------------------------------------
# Parameters
# --------------------------------------------------------------------------

@dataclass
class TankParams:
    liters: float = 150.0
    ua: Optional[float] = None          # W/degC; None -> from UA_LUT

    def ua_value(self) -> float:
        return self.ua if self.ua is not None else ua_from_lut(self.liters)


@dataclass
class CollectorParams:
    area_m2: float = 2.0                # one flat-plate copper panel
    eta0: float = 0.75                  # optical efficiency
    a1: float = 4.0                     # W/m2K, linear loss
    a2: float = 0.015                   # W/m2K2, quadratic loss
    tilt_deg: float = 50.0
    azimuth_deg: float = 0.0            # 0 = south (Open-Meteo convention)


@dataclass
class HeaterParams:
    power_kw: float = 3.0
    eta_el: float = 0.98
    t_max: float = 65.0                 # T_BOILER_MAX


@dataclass
class ScheduleParams:
    run1_ready_by: time = time(19, 0)
    run2_start: time = time(20, 30)
    refill_fraction: float = 0.40       # x: share of tank used before run 2
    t_amb: Optional[float] = None       # refill water temp; None -> forecast air temp at usage time
    t_amb_fallback: float = 20.0        # used when the forecast has no value for that hour
    vacation: bool = False


@dataclass
class ModelParams:
    tank: TankParams = field(default_factory=TankParams)
    collector: CollectorParams = field(default_factory=CollectorParams)
    heater: HeaterParams = field(default_factory=HeaterParams)
    schedule: ScheduleParams = field(default_factory=ScheduleParams)
    model: str = "A"                    # "A" decides by default, "B" optional
    k: Optional[float] = None           # model A gain, degC per kWh/m2; None -> derived from B
    eta_avg: float = 0.5                # average collector efficiency used to derive k
    latitude: float = 32.72
    longitude: float = 35.13
    utc_offset_hours: float = 2.0       # used only for naive datetimes
    confidence_tolerance: float = 0.20  # radiation vs cloud agreement


# --------------------------------------------------------------------------
# Inputs / outputs
# --------------------------------------------------------------------------

@dataclass
class WeatherHour:
    """One hour of Open-Meteo data. Radiation values are W/m2 (hour average)."""
    time: datetime
    temp: float                          # temperature_2m, degC
    cloud_cover: Optional[float] = None  # cloud_cover, 0-100 %
    ghi: Optional[float] = None          # shortwave_radiation
    gti: Optional[float] = None          # global_tilted_irradiance (panel plane)


@dataclass
class RunPlan:
    start: datetime
    end: datetime
    t_from: float
    t_to: float
    kwh: float

    @property
    def minutes(self) -> int:
        return int(round((self.end - self.start).total_seconds() / 60))


@dataclass
class DayPlan:
    created: datetime
    model_used: str
    t_start: float
    t_end_solar: float                   # from the model that decides
    t_end_solar_a: float
    t_end_solar_b: float
    t_end_solar_range: tuple             # (low, high), equal when confidence is high
    confidence: str                      # "high" | "low"
    confidence_note: str
    sunrise: datetime
    sunset: datetime
    run1: Optional[RunPlan]
    run2: Optional[RunPlan]
    total_kwh: float
    t_end_day: float                     # after run 2 (or after usage if no run 2)
    t_next_dawn: float                   # estimate for tomorrow's T_start
    vacation: bool
    warnings: List[str] = field(default_factory=list)
    t_refill: Optional[float] = None     # refill water temp used for the usage step

    @property
    def heating_needed(self) -> bool:
        return self.run1 is not None or self.run2 is not None


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def ua_from_lut(liters: float) -> float:
    """Linear interpolation in UA_LUT, clamped at the ends."""
    sizes = sorted(UA_LUT)
    if liters <= sizes[0]:
        return UA_LUT[sizes[0]]
    if liters >= sizes[-1]:
        return UA_LUT[sizes[-1]]
    for lo, hi in zip(sizes, sizes[1:]):
        if lo <= liters <= hi:
            r = (liters - lo) / (hi - lo)
            return UA_LUT[lo] + r * (UA_LUT[hi] - UA_LUT[lo])
    return UA_LUT[sizes[-1]]


def derive_k(p: ModelParams) -> float:
    """Model A gain from model B parameters (no tank sensor to calibrate)."""
    return p.collector.area_m2 * p.eta_avg * 1000.0 / (p.tank.liters * WH_PER_L_DEG)


def heating_kwh(liters: float, t_from: float, t_to: float, eta_el: float) -> float:
    if t_to <= t_from:
        return 0.0
    return liters * WH_PER_L_DEG * (t_to - t_from) / 1000.0 / eta_el


def mix_after_refill(t: float, fraction: float, t_amb: float) -> float:
    return (1.0 - fraction) * t + fraction * t_amb


def standby(t: float, t_air: float, hours: float, liters: float, ua: float) -> float:
    """Exponential cool-down toward air temperature."""
    if hours <= 0:
        return t
    tau_h = liters * J_PER_L_DEG / ua / 3600.0
    return t_air + (t - t_air) * math.exp(-hours / tau_h)


def _utc_hours(dt: datetime, offset_h: float) -> float:
    if dt.utcoffset() is not None:
        offset_h = dt.utcoffset().total_seconds() / 3600.0
    return dt.hour + dt.minute / 60.0 - offset_h


def solar_elevation_deg(dt: datetime, lat: float, lon: float, offset_h: float) -> float:
    """Approximate solar elevation (good to ~1 deg), dt = middle of the hour."""
    doy = dt.timetuple().tm_yday
    g = 2 * math.pi / 365.0 * (doy - 1 + (_utc_hours(dt, offset_h) - 12) / 24)
    decl = (0.006918 - 0.399912 * math.cos(g) + 0.070257 * math.sin(g)
            - 0.006758 * math.cos(2 * g) + 0.000907 * math.sin(2 * g))
    eqt = 229.18 * (0.000075 + 0.001868 * math.cos(g) - 0.032077 * math.sin(g)
                    - 0.014615 * math.cos(2 * g) - 0.040849 * math.sin(2 * g))
    solar_time_min = _utc_hours(dt, offset_h) * 60 + eqt + 4 * lon
    ha = math.radians(solar_time_min / 4 - 180)
    la = math.radians(lat)
    sin_el = math.sin(la) * math.sin(decl) + math.cos(la) * math.cos(decl) * math.cos(ha)
    return math.degrees(math.asin(max(-1.0, min(1.0, sin_el))))


def clear_sky_ghi(dt: datetime, lat: float, lon: float, offset_h: float) -> float:
    """Haurwitz clear-sky GHI, W/m2."""
    mid = dt + timedelta(minutes=30)
    el = solar_elevation_deg(mid, lat, lon, offset_h)
    if el <= 0:
        return 0.0
    cz = math.sin(math.radians(el))
    return 1098.0 * cz * math.exp(-0.059 / cz)


def ghi_from_clouds(ghi_clear: float, cloud_pct: float) -> float:
    """Kasten-Czeplak: GHI = GHI_clear * (1 - 0.75 * C^3.4)."""
    c = max(0.0, min(1.0, cloud_pct / 100.0))
    return ghi_clear * (1.0 - 0.75 * c ** 3.4)


def _on_date(d: datetime, t: time) -> datetime:
    return d.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)


# --------------------------------------------------------------------------
# Solar phase
# --------------------------------------------------------------------------

def _daylight(hours: List[WeatherHour], sunrise: datetime, sunset: datetime) -> List[WeatherHour]:
    return [h for h in hours if h.time + timedelta(hours=1) > sunrise and h.time < sunset]


def _fraction_of_hour_in_daylight(h: WeatherHour, sunrise: datetime, sunset: datetime) -> float:
    start = max(h.time, sunrise)
    end = min(h.time + timedelta(hours=1), sunset)
    return max(0.0, (end - start).total_seconds() / 3600.0)


def plane_irradiance(h: WeatherHour, p: ModelParams) -> Optional[float]:
    """Radiation on the panel: GTI first, then GHI, then cloud estimate."""
    if h.gti is not None:
        return h.gti
    if h.ghi is not None:
        return h.ghi
    if h.cloud_cover is not None:
        return ghi_from_clouds(clear_sky_ghi(h.time, p.latitude, p.longitude, p.utc_offset_hours),
                               h.cloud_cover)
    return None


def check_confidence(hours: List[WeatherHour], p: ModelParams):
    """Compare measured/forecast GHI with the cloud-cover estimate over the day."""
    meas, est = 0.0, 0.0
    have_ghi = have_cc = False
    for h in hours:
        if h.ghi is not None:
            have_ghi = True
        if h.cloud_cover is not None:
            have_cc = True
        if h.ghi is not None and h.cloud_cover is not None:
            meas += h.ghi
            est += ghi_from_clouds(clear_sky_ghi(h.time, p.latitude, p.longitude, p.utc_offset_hours),
                                   h.cloud_cover)
    if not have_ghi and not have_cc and all(h.gti is None for h in hours):
        return "low", "no radiation or cloud data", None
    if not have_ghi or not have_cc:
        return "low", "only one source available, no cross-check", None
    if est < 1.0:
        return "high", "no daylight radiation expected", 1.0
    ratio = meas / est
    if abs(ratio - 1.0) <= p.confidence_tolerance:
        return "high", f"radiation and clouds agree ({ratio:.0%})", ratio
    return "low", f"radiation is {ratio:.0%} of the cloud estimate", ratio


def solar_model_b(t_start: float, hours: List[WeatherHour], sunrise: datetime, sunset: datetime,
                  p: ModelParams, scale: float = 1.0) -> float:
    """Physical collector model, 10-minute steps."""
    c, tk = p.collector, p.tank
    ua = tk.ua_value()
    m_c = tk.liters * J_PER_L_DEG
    t = t_start
    for h in _daylight(hours, sunrise, sunset):
        frac = _fraction_of_hour_in_daylight(h, sunrise, sunset)
        g = plane_irradiance(h, p)
        g = 0.0 if g is None else g * scale
        steps = max(1, int(round(frac * 6)))
        dt_s = frac * 3600.0 / steps
        for _ in range(steps):
            dtemp = t - h.temp
            eta = c.eta0 - c.a1 * dtemp / g - c.a2 * dtemp ** 2 / g if g > 1.0 else 0.0
            q_solar = c.area_m2 * g * max(0.0, eta)
            q_loss = ua * (t - h.temp)
            t += (q_solar - q_loss) * dt_s / m_c
    return t


def solar_model_a(t_start: float, hours: List[WeatherHour], sunrise: datetime, sunset: datetime,
                  p: ModelParams, scale: float = 1.0) -> float:
    """Simple model: dT = k * H_day - standby loss over daylight."""
    k = p.k if p.k is not None else derive_k(p)
    day = _daylight(hours, sunrise, sunset)
    h_day = 0.0
    for h in day:
        g = plane_irradiance(h, p)
        if g is not None:
            h_day += g * scale * _fraction_of_hour_in_daylight(h, sunrise, sunset) / 1000.0
    t_gain = t_start + k * h_day
    t_air = sum(h.temp for h in day) / len(day) if day else t_start
    hours_light = (sunset - sunrise).total_seconds() / 3600.0
    # loss evaluated on the mean of start and end temperature
    t_mean = (t_start + t_gain) / 2.0
    loss = p.tank.ua_value() * (t_mean - t_air) * hours_light * 3600.0 / (p.tank.liters * J_PER_L_DEG)
    return t_gain - loss


# --------------------------------------------------------------------------
# Day plan
# --------------------------------------------------------------------------

def plan_day(now: datetime, hours: List[WeatherHour], sunrise: datetime, sunset: datetime,
             t_start: float, p: Optional[ModelParams] = None) -> DayPlan:
    """
    Build the day plan. Call it at dawn and again every hour until
    replan_due() says stop (dusk, or run 1 has to start).
    """
    p = p or ModelParams()
    s, ht, tk = p.schedule, p.heater, p.tank
    warnings: List[str] = []

    if not _daylight(hours, sunrise, sunset):
        warnings.append("no weather data for daylight hours, solar gain assumed zero")

    conf, note, ratio = check_confidence(_daylight(hours, sunrise, sunset), p)

    t_a = solar_model_a(t_start, hours, sunrise, sunset, p)
    t_b = solar_model_b(t_start, hours, sunrise, sunset, p)
    solar_fn = solar_model_b if p.model.upper() == "B" else solar_model_a
    t_end_solar = t_b if p.model.upper() == "B" else t_a

    # Range when the two sources disagree: rerun with radiation scaled to the cloud estimate.
    if conf == "low" and ratio:
        alt = solar_fn(t_start, hours, sunrise, sunset, p, scale=1.0 / ratio)
        t_range = (round(min(t_end_solar, alt), 1), round(max(t_end_solar, alt), 1))
    else:
        t_range = (round(t_end_solar, 1), round(t_end_solar, 1))

    run1 = run2 = None
    t_amb = None
    ua = tk.ua_value()
    night_air = _night_air(hours, sunset, s.t_amb_fallback)
    ready_by = _on_date(sunset, s.run1_ready_by)
    run2_start = _on_date(sunset, s.run2_start)
    t = t_end_solar

    if s.vacation:
        # No electric heating at all and no planned usage: tank just cools from dusk.
        t_end_day = t
        end_of_day = sunset
    else:
        # Standby from dusk to the time run 1 must be done (bounded below by dusk).
        t_at_ready = standby(t, night_air, max(0.0, (ready_by - sunset).total_seconds() / 3600),
                             tk.liters, ua)
        kwh1 = heating_kwh(tk.liters, t_at_ready, ht.t_max, ht.eta_el)
        if kwh1 > 0:
            dur = timedelta(hours=kwh1 / ht.power_kw)
            start1 = ready_by - dur
            if start1 < sunset:
                warnings.append("run 1 must start before dusk, it overlaps the solar phase")
            if start1 < now:
                warnings.append("run 1 start time is already past")
            run1 = RunPlan(start1, ready_by, round(t_at_ready, 1), ht.t_max, round(kwh1, 2))
            t = ht.t_max
        else:
            t = t_at_ready

        if run2_start < ready_by:
            warnings.append("run 2 is set to start before run 1 is ready")
        hrs_gap = max(0.0, (run2_start - ready_by).total_seconds() / 3600)
        t = standby(t, night_air, hrs_gap, tk.liters, ua)
        t_amb = refill_temperature(hours, ready_by, run2_start, s)
        t = mix_after_refill(t, s.refill_fraction, t_amb)
        kwh2 = heating_kwh(tk.liters, t, ht.t_max, ht.eta_el)
        if kwh2 > 0:
            dur2 = timedelta(hours=kwh2 / ht.power_kw)
            run2 = RunPlan(run2_start, run2_start + dur2, round(t, 1), ht.t_max, round(kwh2, 2))
            t = ht.t_max
        t_end_day = t
        end_of_day = run2.end if run2 else run2_start

    next_sunrise = sunrise + timedelta(days=1)
    t_next_dawn = standby(t_end_day, night_air,
                          max(0.0, (next_sunrise - end_of_day).total_seconds() / 3600), tk.liters, ua)

    total = (run1.kwh if run1 else 0.0) + (run2.kwh if run2 else 0.0)
    return DayPlan(
        created=now, model_used=p.model.upper(), t_start=t_start,
        t_end_solar=round(t_end_solar, 1), t_end_solar_a=round(t_a, 1), t_end_solar_b=round(t_b, 1),
        t_end_solar_range=t_range, confidence=conf, confidence_note=note,
        sunrise=sunrise, sunset=sunset, run1=run1, run2=run2, total_kwh=round(total, 2),
        t_end_day=round(t_end_day, 1), t_next_dawn=round(t_next_dawn, 1),
        vacation=s.vacation, warnings=warnings,
        t_refill=None if t_amb is None else round(t_amb, 1),
    )


def refill_temperature(hours: List[WeatherHour], use_from: datetime, use_to: datetime,
                       s: ScheduleParams) -> float:
    """Refill water temperature: fixed if set, else forecast air temp during usage."""
    if s.t_amb is not None:
        return s.t_amb
    window = [h.temp for h in hours
              if h.time < use_to and h.time + timedelta(hours=1) > use_from]
    if not window:
        window = [h.temp for h in hours if h.time <= use_to < h.time + timedelta(hours=1)]
    return sum(window) / len(window) if window else s.t_amb_fallback


def _night_air(hours: List[WeatherHour], sunset: datetime, default: float) -> float:
    night = [h.temp for h in hours if h.time >= sunset]
    return sum(night) / len(night) if night else default


def replan_due(now: datetime, plan: DayPlan) -> bool:
    """Replan hourly until dusk, or until run 1 has to start - whichever is first."""
    stop = plan.sunset
    if plan.run1 is not None:
        stop = min(stop, plan.run1.start)
    return now < stop


def should_publish(plan: DayPlan) -> bool:
    """Send a plan only when electric heating has to turn on."""
    return not plan.vacation and plan.heating_needed


def plan_summary(plan: DayPlan) -> List[str]:
    """Short status lines, problems first."""
    lines = []
    for w in plan.warnings:
        lines.append(f"RED   {w}")
    if plan.confidence == "low":
        lo, hi = plan.t_end_solar_range
        lines.append(f"YEL   low confidence: {plan.confidence_note}; dusk {lo}-{hi} C")
    lines.append(f"GRN   solar {plan.t_start} -> {plan.t_end_solar} C at dusk "
                 f"(A {plan.t_end_solar_a}, B {plan.t_end_solar_b})")
    if plan.vacation:
        lines.append("GRN   vacation: no electric heating")
    for name, r in (("run 1", plan.run1), ("run 2", plan.run2)):
        if r:
            extra = f" (refill at {plan.t_refill} C)" if name == "run 2" and plan.t_refill is not None else ""
            lines.append(f"GRN   {name}: {r.start:%H:%M}-{r.end:%H:%M} "
                         f"{r.t_from}->{r.t_to} C, {r.kwh} kWh, {r.minutes} min{extra}")
        elif not plan.vacation:
            lines.append(f"GRN   {name}: not needed")
    lines.append(f"GRN   total {plan.total_kwh} kWh; tomorrow dawn ~{plan.t_next_dawn} C")
    return lines
