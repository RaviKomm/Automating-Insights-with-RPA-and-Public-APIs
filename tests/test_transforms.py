import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from onboarding_weather_bot import (
    celsius_to_fahrenheit, flag_risk_day, compute_city_stats,
    assign_users_to_cities, recommend_onboarding, build_day_records,
)
from collections import Counter


def test_celsius_to_fahrenheit_freezing():
    assert celsius_to_fahrenheit(0) == 32.0

def test_celsius_to_fahrenheit_boiling():
    assert celsius_to_fahrenheit(100) == 212.0

def test_celsius_to_fahrenheit_body_temp():
    assert abs(celsius_to_fahrenheit(37) - 98.6) < 0.01

def test_flag_risk_day_cold():
    # temp_max_f < 20 → HIGH RISK
    assert flag_risk_day(15.0, 5.0, 0, 0, 0, 0) == "HIGH RISK"

def test_flag_risk_day_hot():
    # temp_max_f > 95 → HIGH RISK
    assert flag_risk_day(96.0, 70.0, 0, 0, 0, 0) == "HIGH RISK"

def test_flag_risk_day_heavy_rain():
    assert flag_risk_day(72.0, 60.0, 25.0, 0, 0, 0) == "HIGH RISK"

def test_flag_risk_day_high_wind():
    assert flag_risk_day(72.0, 60.0, 0, 55.0, 0, 0) == "HIGH RISK"

def test_flag_risk_day_high_uv():
    assert flag_risk_day(72.0, 60.0, 0, 0, 9.0, 0) == "HIGH RISK"

def test_flag_risk_day_high_pm10():
    assert flag_risk_day(72.0, 60.0, 0, 0, 0, 55.0) == "HIGH RISK"

def test_flag_risk_day_safe():
    assert flag_risk_day(72.0, 55.0, 2.0, 15.0, 4.0, 20.0) == "LOW RISK"

def test_flag_risk_day_boundary_exact():
    # Exact threshold values are NOT high risk (thresholds are strict >/<)
    assert flag_risk_day(20.0, 20.0, 20.0, 50.0, 8.0, 50.0) == "LOW RISK"


def test_risk_score_all_high():
    days = [{"Risk_Flag": "HIGH RISK", "Precipitation_mm": 0, "UV_Index": 0,
             "TempMax_F": 0, "TempMin_F": 0} for _ in range(7)]
    stats = compute_city_stats("TestCity", days)
    assert stats["risk_score"] == 100.0

def test_risk_score_all_low():
    days = [{"Risk_Flag": "LOW RISK", "Precipitation_mm": 0, "UV_Index": 0,
             "TempMax_F": 70, "TempMin_F": 50} for _ in range(7)]
    stats = compute_city_stats("TestCity", days)
    assert stats["risk_score"] == 0.0

def test_risk_score_partial():
    # 3 out of 7 high risk = ~42.86%
    days = (
        [{"Risk_Flag": "HIGH RISK", "Precipitation_mm": 5.0, "UV_Index": 3.0,
          "TempMax_F": 70.0, "TempMin_F": 50.0}] * 3 +
        [{"Risk_Flag": "LOW RISK",  "Precipitation_mm": 1.0, "UV_Index": 2.0,
          "TempMax_F": 65.0, "TempMin_F": 45.0}] * 4
    )
    stats = compute_city_stats("TestCity", days)
    assert abs(stats["risk_score"] - (3 / 7 * 100)) < 0.01

def test_avg_precipitation():
    days = [
        {"Risk_Flag": "LOW RISK", "Precipitation_mm": 10.0, "UV_Index": 2.0,
         "TempMax_F": 70.0, "TempMin_F": 50.0},
        {"Risk_Flag": "LOW RISK", "Precipitation_mm": 20.0, "UV_Index": 2.0,
         "TempMax_F": 70.0, "TempMin_F": 50.0},
    ]
    stats = compute_city_stats("TestCity", days)
    assert stats["avg_precip"] == 15.0

def test_temp_variance():
    days = [
        {"Risk_Flag": "LOW RISK", "Precipitation_mm": 0, "UV_Index": 2.0,
         "TempMax_F": 80.0, "TempMin_F": 40.0},
        {"Risk_Flag": "LOW RISK", "Precipitation_mm": 0, "UV_Index": 2.0,
         "TempMax_F": 70.0, "TempMin_F": 50.0},
    ]
    stats = compute_city_stats("TestCity", days)
    # variance = max(all_max) - min(all_min) = 80 - 40 = 40
    assert stats["temp_variance"] == 40.0


def test_round_robin_assignment():
    users = [{"name": f"User{i}", "email": f"u{i}@test.com"} for i in range(10)]
    city_names = ["Minneapolis", "Chicago", "New York", "Austin", "Denver"]
    assignments = assign_users_to_cities(users, city_names)
    counts = Counter(a["Assigned_City"] for a in assignments)
    assert all(counts[c] == 2 for c in city_names)

def test_round_robin_order():
    users = [{"name": f"User{i}", "email": f"u{i}@test.com"} for i in range(5)]
    city_names = ["A", "B", "C", "D", "E"]
    assignments = assign_users_to_cities(users, city_names)
    assert assignments[0]["Assigned_City"] == "A"
    assert assignments[1]["Assigned_City"] == "B"
    assert assignments[4]["Assigned_City"] == "E"

def test_recommend_remote_above_threshold():
    # risk_score > 40% → REMOTE
    assert recommend_onboarding(41.0) == "REMOTE"

def test_recommend_in_person_at_threshold():
    # Exactly 40% → IN-PERSON (threshold is strict >)
    assert recommend_onboarding(40.0) == "IN-PERSON"

def test_recommend_in_person_below_threshold():
    assert recommend_onboarding(28.5) == "IN-PERSON"


def test_build_day_records_merges_on_date():
    forecast = {
        "dates":    ["2026-04-26", "2026-04-27"],
        "temp_max": [20.0, 22.0],
        "temp_min": [10.0, 12.0],
        "precip":   [0.0,  5.0],
        "wind":     [15.0, 20.0],
    }
    aq = {
        "dates":    ["2026-04-26", "2026-04-27"],
        "uv_index": [3.0, 4.0],
        "pm10":     [18.0, 22.0],
    }
    records = build_day_records("Chicago", forecast, aq)
    assert len(records) == 2
    assert records[0]["City"] == "Chicago"
    assert records[0]["Date"] == "2026-04-26"
    assert abs(records[0]["TempMax_F"] - celsius_to_fahrenheit(20.0)) < 0.01
    assert records[0]["UV_Index"] == 3.0

def test_build_day_records_defaults_missing_aq_date():
    # AQ only has 1 day; forecast has 2 — missing AQ day must default to 0
    forecast = {
        "dates":    ["2026-04-26", "2026-04-27"],
        "temp_max": [20.0, 22.0],
        "temp_min": [10.0, 12.0],
        "precip":   [0.0,  5.0],
        "wind":     [15.0, 20.0],
    }
    aq = {
        "dates":    ["2026-04-26"],
        "uv_index": [3.0],
        "pm10":     [18.0],
    }
    records = build_day_records("Chicago", forecast, aq)
    assert len(records) == 2
    assert records[1]["UV_Index"] == 0.0
    assert records[1]["PM10"] == 0.0

def test_build_day_records_sets_risk_flag():
    # UV 9.0 > threshold of 8 → HIGH RISK
    forecast = {
        "dates": ["2026-04-26"], "temp_max": [25.0], "temp_min": [15.0],
        "precip": [0.0], "wind": [10.0],
    }
    aq = {"dates": ["2026-04-26"], "uv_index": [9.0], "pm10": [10.0]}
    records = build_day_records("Chicago", forecast, aq)
    assert records[0]["Risk_Flag"] == "HIGH RISK"
