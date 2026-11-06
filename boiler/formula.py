"""The formula: weather in, boiler minutes out. Pure functions, no I/O, no prints.

Parameters live in a Formula object (not in code) so a UI can edit them and a backtest can
compare several of them on the same days.
"""
from dataclasses import dataclass, field, asdict
from statistics import mean


@dataclass
class Formula:
    # outdoor mean temperature (°C) -> base ON minutes, linearly interpolated between points
    temp_lut: dict = field(default_factory=lambda: {
        6: 180, 8: 180, 10: 160, 12: 150, 14: 130, 16: 100,
        18: 60, 20: 40, 22: 20, 24: 10, 26: 0, 28: 0, 30: 0,
    })
    cloud_k: float = 0.6          # max extra time from clouds (0.6 = +60%)
    sun_min_temp: float = 10.0    # below this, clouds are ignored (no sun to lose)
    sun_max_temp: float = 20.0    # at/above this, clouds count fully
    max_on: int = 180             # hard cap, minutes
    window_start_offset: int = 2  # analysis window: sunrise + N hours ...
    window_end_offset: int = 1    # ... to sunset - N hours

    def to_dict(self) -> dict:
        d = asdict(self)
        d["temp_lut"] = {str(k): v for k, v in self.temp_lut.items()}
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Formula":
        d = dict(d)
        if "temp_lut" in d:
            d["temp_lut"] = {float(k): v for k, v in d["temp_lut"].items()}
        return cls(**d)

    def validate(self) -> list:
        """Human-readable problems with these parameters (empty list = fine)."""
        problems = []
        pts = sorted(self.temp_lut.items())
        if len(pts) < 2:
            problems.append("Temperature table needs at least 2 points.")
        if any(v < 0 for _, v in pts):
            problems.append("Minutes cannot be negative.")
        if any(b[1] > a[1] for a, b in zip(pts, pts[1:])):
            problems.append("Warmer weather should never need MORE boiler time (table is not decreasing).")
        if any(v > self.max_on for _, v in pts):
            problems.append(f"Table has values above the max cap ({self.max_on} min); they will be cut.")
        if not 0 <= self.cloud_k <= 2:
            problems.append("Cloud factor should be between 0 and 2.")
        if self.sun_min_temp >= self.sun_max_temp:
            problems.append("Sun-sensitivity range is empty (min temp must be below max temp).")
        return problems


def base_minutes(T: float, f: Formula) -> float:
    """Temperature-only duration, interpolated from the table."""
    temps = sorted(f.temp_lut)
    if T <= temps[0]:
        return f.temp_lut[temps[0]]
    if T >= temps[-1]:
        return f.temp_lut[temps[-1]]
    for t0, t1 in zip(temps, temps[1:]):
        if t0 <= T <= t1:
            v0, v1 = f.temp_lut[t0], f.temp_lut[t1]
            return v0 + (T - t0) / (t1 - t0) * (v1 - v0)
    raise ValueError("unreachable: temperature not bracketed")


def cloud_correction(T: float, CC: float, f: Formula) -> float:
    """alpha: fractional extra time from clouds. Zero when it's too cold for sun to matter."""
    st = (T - f.sun_min_temp) / (f.sun_max_temp - f.sun_min_temp)
    st = max(0.0, min(1.0, st))
    return f.cloud_k * (CC / 100.0) * st


def compute_duration(T: float, CC: float, f: Formula = None) -> int:
    """Boiler ON minutes for mean temperature T (°C) and mean cloud cover CC (%)."""
    f = f or Formula()
    base = base_minutes(T, f)
    alpha = cloud_correction(T, CC, f)
    return min(int(round(base * (1 + alpha))), f.max_on)


def explain(T: float, CC: float, f: Formula = None) -> dict:
    """The same calculation with every step visible (for logs and the UI)."""
    f = f or Formula()
    base = base_minutes(T, f)
    alpha = cloud_correction(T, CC, f)
    return {"temp": T, "clouds": CC, "base_min": round(base, 1), "cloud_alpha": round(alpha, 3),
            "result_min": compute_duration(T, CC, f)}


def daily_mean(data, sunrise: int, sunset: int, f: Formula = None):
    """Mean temperature and cloud cover in [sunrise+start_offset, sunset-end_offset].
    Returns (None, None) when there is no data in that window."""
    f = f or Formula()
    if not data:
        return None, None
    lo = sunrise + f.window_start_offset
    hi = sunset - f.window_end_offset
    rows = [d for d in data if lo <= d["time"].hour <= hi]
    if not rows:
        return None, None
    return mean(d["temp"] for d in rows), mean(d["clouds"] for d in rows)
