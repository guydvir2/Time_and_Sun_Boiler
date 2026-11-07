"""Tests for boiler_model. Run: python -m pytest tests -q"""

import math
import os
import sys
from datetime import datetime, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boiler_model as bm  # noqa: E402

DAY = datetime(2026, 11, 8)
SUNRISE = DAY.replace(hour=6, minute=30)
SUNSET = DAY.replace(hour=16, minute=30)


def make_day(peak_w=550.0, clouds=10.0, temp=22.0, ghi_factor=1.0, night_temp=16.0):
    """24 hourly records: sine radiation between sunrise and sunset."""
    hours = []
    for hr in range(24):
        t = DAY.replace(hour=hr)
        mid = hr + 0.5
        if 6.5 < mid < 16.5:
            g = peak_w * math.sin(math.pi * (mid - 6.5) / 10.0)
        else:
            g = 0.0
        is_night = hr >= 17 or hr < 6
        hours.append(bm.WeatherHour(time=t, temp=night_temp if is_night else temp,
                                    cloud_cover=clouds, ghi=g * ghi_factor, gti=g))
    return hours


def test_ua_lut_interpolates():
    assert bm.ua_from_lut(150) == 1.7
    assert abs(bm.ua_from_lut(175) - 1.85) < 1e-9
    assert bm.ua_from_lut(40) == 1.1


def test_derived_k_matches_document():
    assert abs(bm.derive_k(bm.ModelParams()) - 5.7) < 0.05


def test_heating_energy_and_mix():
    assert abs(bm.heating_kwh(150, 55, 65, 1.0) - 1.7445) < 1e-3
    assert abs(bm.mix_after_refill(65, 0.4, 20) - 47.0) < 1e-9


def test_solar_raises_tank_and_run1_ends_at_ready_time():
    plan = bm.plan_day(DAY.replace(hour=7), make_day(), SUNRISE, SUNSET, t_start=30.0)
    assert plan.t_end_solar > 30.0
    assert plan.run1 is not None
    assert plan.run1.end == DAY.replace(hour=19)
    assert plan.run1.start < plan.run1.end
    assert plan.run2 is not None and plan.run2.start == DAY.replace(hour=20, minute=30)
    assert plan.total_kwh > 0


def test_hot_tank_skips_run1():
    plan = bm.plan_day(DAY.replace(hour=7), make_day(), SUNRISE, SUNSET, t_start=70.0)
    assert plan.run1 is None
    assert plan.run2 is not None


def test_vacation_has_no_electric_and_is_not_published():
    p = bm.ModelParams(schedule=bm.ScheduleParams(vacation=True))
    plan = bm.plan_day(DAY.replace(hour=7), make_day(), SUNRISE, SUNSET, t_start=30.0, p=p)
    assert plan.run1 is None and plan.run2 is None
    assert plan.total_kwh == 0
    assert not bm.should_publish(plan)


def test_storm_day_needs_more_electric_than_clear_day():
    clear = bm.plan_day(DAY, make_day(peak_w=550), SUNRISE, SUNSET, t_start=30.0)
    storm = bm.plan_day(DAY, make_day(peak_w=150, clouds=95), SUNRISE, SUNSET, t_start=30.0)
    assert storm.total_kwh > clear.total_kwh


def test_model_b_efficiency_drops_for_hot_tank():
    p = bm.ModelParams()
    hours = make_day()
    gain_cold = bm.solar_model_b(25.0, hours, SUNRISE, SUNSET, p) - 25.0
    gain_hot = bm.solar_model_b(60.0, hours, SUNRISE, SUNSET, p) - 60.0
    assert gain_hot < gain_cold


def test_confidence_low_when_radiation_and_clouds_disagree():
    plan = bm.plan_day(DAY, make_day(clouds=5, ghi_factor=0.3), SUNRISE, SUNSET, t_start=30.0)
    assert plan.confidence == "low"
    lo, hi = plan.t_end_solar_range
    assert lo < hi


def test_replan_stops_at_run1_start_or_dusk():
    plan = bm.plan_day(DAY.replace(hour=7), make_day(), SUNRISE, SUNSET, t_start=30.0)
    stop = min(SUNSET, plan.run1.start)
    assert bm.replan_due(stop.replace(minute=0) if stop.minute else stop.replace(hour=stop.hour - 1), plan)
    assert not bm.replan_due(stop, plan)


def test_publish_only_when_heating_needed():
    p = bm.ModelParams(heater=bm.HeaterParams(t_max=40.0),
                       schedule=bm.ScheduleParams(refill_fraction=0.0))
    plan = bm.plan_day(DAY, make_day(), SUNRISE, SUNSET, t_start=60.0, p=p)
    assert not plan.heating_needed
    assert not bm.should_publish(plan)


def test_cloud_fallback_when_radiation_missing():
    hours = [bm.WeatherHour(time=h.time, temp=h.temp, cloud_cover=20.0) for h in make_day()]
    plan = bm.plan_day(DAY, hours, SUNRISE, SUNSET, t_start=30.0)
    assert plan.t_end_solar > 30.0
    assert plan.confidence == "low"
