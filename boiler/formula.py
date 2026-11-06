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
        d["temp_lut"] = {f"{float(k):g}": v for k, v in self.temp_lut.items()}   # 6.0 -> "6"
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

    def blocking_problems(self) -> list:
        """Problems that must stop a save (the 'above cap' note is only a warning)."""
        return [p for p in self.validate() if "will be cut" not in p]


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


# ----------------------------------------------------------------------------- saved formula
# The tuned formula lives in stored_data/formula.json (written by the /tune page).
# The engine reads it at every daily run, so a saved change applies from the next run, no restart.

LIMITS = {"cloud_k": (0, 2), "sun_min_temp": (-10, 40), "sun_max_temp": (-10, 45), "max_on": (0, 600),
          "window_start_offset": (0, 6), "window_end_offset": (0, 6)}


def parse_formula(d) -> "Formula":
    """Build a Formula from untrusted input (the web page). Raises ValueError with a readable message."""
    if not isinstance(d, dict):
        raise ValueError("formula must be an object")
    out = Formula().to_dict()
    for k, (lo, hi) in LIMITS.items():
        if k in d:
            v = d[k]
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not lo <= v <= hi:
                raise ValueError(f"{k} must be a number from {lo} to {hi}")
            out[k] = int(v) if k in ("max_on", "window_start_offset", "window_end_offset") else float(v)
    if "temp_lut" in d:
        lut = d["temp_lut"]
        if not isinstance(lut, dict) or not 2 <= len(lut) <= 40:
            raise ValueError("temp_lut must have 2 to 40 points")
        clean = {}
        for k, v in lut.items():
            t = float(k)
            if not -20 <= t <= 50 or isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 <= v <= 600:
                raise ValueError("temp_lut: temperatures -20..50, minutes 0..600")
            clean[f"{t:g}"] = round(float(v), 1)
        out["temp_lut"] = clean
    return Formula.from_dict(out)


def load_formula(s) -> "Formula":
    """The saved formula, or the built-in default if none is saved (or the file is broken)."""
    import json
    try:
        with open(s.formula_file) as f:
            return parse_formula(json.load(f))
    except FileNotFoundError:
        return Formula()
    except (ValueError, OSError) as e:
        print(f"⚠️  {s.formula_file} is unreadable ({e}); using the default formula")
        return Formula()


def save_formula(s, f: "Formula", who="dashboard"):
    """Write the formula and append the previous one to formula_history.jsonl (so any change can be undone)."""
    import json, os
    from datetime import datetime
    os.makedirs(os.path.dirname(s.formula_file), exist_ok=True)
    old = load_formula(s)
    with open(s.formula_history, "a") as h:
        h.write(json.dumps({"time": datetime.now().isoformat(timespec="seconds"), "who": who,
                            "old": old.to_dict(), "new": f.to_dict()}) + "\n")
    tmp = s.formula_file + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(f.to_dict(), fh, indent=1)
    os.replace(tmp, s.formula_file)
