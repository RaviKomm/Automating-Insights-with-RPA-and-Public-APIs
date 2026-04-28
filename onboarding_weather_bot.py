"""
onboarding_weather_bot.py
RPA bot for HR scheduling intelligence — fetches 7-day weather and air
quality data for 5 office locations and produces onboarding risk reports.
"""

import requests
import csv
import html
from datetime import datetime, timezone, timedelta
from statistics import mean

# ---------------------------------------------------------------------------
# CONSTANTS — all business-rule thresholds live here for easy reconfiguration
# ---------------------------------------------------------------------------

# Risk thresholds (a day is HIGH RISK if ANY single threshold is exceeded)
TEMP_MIN_F        = 20.0   # °F — dangerously cold
TEMP_MAX_F        = 95.0   # °F — dangerously hot
PRECIP_MAX_MM     = 20.0   # mm — heavy precipitation
WIND_MAX_KMH      = 50.0   # km/h — strong winds
UV_MAX            = 8.0    # UV index — high UV exposure
PM10_MAX          = 50.0   # µg/m³ — poor air quality

# A city's 7-day risk score above this % triggers a REMOTE recommendation
RISK_SCORE_THRESHOLD = 40.0

# Office city coordinates (lat, lon)
CITIES = {
    "Minneapolis": {"lat": 44.98, "lon": -93.27},
    "Chicago":     {"lat": 41.88, "lon": -87.63},
    "New York":    {"lat": 40.71, "lon": -74.01},
    "Austin":      {"lat": 30.27, "lon": -97.74},
    "Denver":      {"lat": 39.74, "lon": -104.98},
}

# API endpoints
FORECAST_URL    = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
USERS_URL       = "https://jsonplaceholder.typicode.com/users"
API_TIMEOUT     = 10  # seconds per request


# ---------------------------------------------------------------------------
# TRANSFORM FUNCTIONS — pure functions, no side effects
# ---------------------------------------------------------------------------

def celsius_to_fahrenheit(celsius):
    """Convert Celsius to Fahrenheit. Formula: (C × 9/5) + 32."""
    return (celsius * 9 / 5) + 32


def flag_risk_day(temp_max_f, temp_min_f, precip, wind, uv, pm10):
    """
    Classify a single day as HIGH RISK or LOW RISK based on HR policy thresholds.
    A day is HIGH RISK if ANY single condition exceeds its threshold — one bad
    metric is enough to make outdoor/commute conditions unsafe for new hires.
    """
    if temp_max_f < TEMP_MIN_F:
        return "HIGH RISK"   # daytime high is still dangerously cold
    if temp_min_f < TEMP_MIN_F:
        return "HIGH RISK"   # overnight low is dangerously cold
    if temp_max_f > TEMP_MAX_F:
        return "HIGH RISK"   # dangerously hot
    if precip > PRECIP_MAX_MM:
        return "HIGH RISK"   # heavy precipitation
    if wind > WIND_MAX_KMH:
        return "HIGH RISK"   # strong winds
    if uv > UV_MAX:
        return "HIGH RISK"   # high UV exposure
    if pm10 > PM10_MAX:
        return "HIGH RISK"   # poor air quality
    return "LOW RISK"


def compute_city_stats(city_name, day_records):
    """
    Aggregate 7 day records into city-level statistics for HR decision-making.

    Args:
        city_name: string name of the city
        day_records: list of dicts, each with keys:
                     Risk_Flag, TempMax_F, TempMin_F, Precipitation_mm, UV_Index

    Returns dict with city, risk_score (%), avg_precip (mm), avg_uv, temp_variance (°F range)
    """
    high_risk_count = sum(1 for d in day_records if d["Risk_Flag"] == "HIGH RISK")

    # Risk score = percentage of 7 forecast days that are high risk
    risk_score = (high_risk_count / len(day_records)) * 100 if day_records else 0.0

    # Average daily precipitation across the forecast window
    avg_precip = mean(d["Precipitation_mm"] for d in day_records) if day_records else 0.0

    # Average UV index across the forecast window
    avg_uv = mean(d["UV_Index"] for d in day_records) if day_records else 0.0

    # Temperature variance: full range from coldest low to hottest high across all 7 days.
    # Identifies cities with extreme swings that complicate dress-code guidance for new hires.
    all_max = [d["TempMax_F"] for d in day_records]
    all_min = [d["TempMin_F"] for d in day_records]
    temp_variance = max(all_max) - min(all_min) if day_records else 0.0

    return {
        "city":          city_name,
        "risk_score":    round(risk_score, 2),
        "avg_precip":    round(avg_precip, 2),
        "avg_uv":        round(avg_uv, 2),
        "temp_variance": round(temp_variance, 2),
    }


def assign_users_to_cities(users, city_names):
    """
    Round-robin assignment of new hires to office cities.
    Simulates the HR system distributing onboarding cohorts across locations.

    Args:
        users: list of dicts with 'name' and 'email' keys
        city_names: ordered list of city name strings

    Returns list of dicts with Employee_Name, Employee_Email, Assigned_City
    """
    num_cities = len(city_names)
    assignments = []
    for i, user in enumerate(users):
        assigned_city = city_names[i % num_cities]  # cycle through cities
        assignments.append({
            "Employee_Name":  user.get("name", "Unknown"),
            "Employee_Email": user.get("email", "unknown@example.com"),
            "Assigned_City":  assigned_city,
        })
    return assignments


def recommend_onboarding(city_risk_score):
    """
    Determine onboarding mode based on city's 7-day risk score.
    HR policy: cities where >40% of upcoming days are HIGH RISK require
    remote onboarding to protect new hire health and first impressions.
    """
    if city_risk_score > RISK_SCORE_THRESHOLD:
        return "REMOTE"
    return "IN-PERSON"


def build_day_records(city_name, forecast, aq):
    """
    Merge forecast and air quality data for one city, aligned by calendar date.

    The two APIs return separate date arrays that may not be identical in length.
    We align on forecast dates (the primary source) and use 0.0 as a safe default
    for any AQ date that is missing — better to under-flag risk than crash.

    Returns list of day-record dicts ready to write to CSV and HTML.
    """
    # Build a lookup dict for AQ data so we can join on date rather than index.
    # This guards against length mismatches between the two API responses.
    aq_by_date = {}
    uv_list  = aq.get("uv_index", [])
    pm_list  = aq.get("pm10", [])
    for i, date in enumerate(aq.get("dates", [])):
        aq_by_date[date] = {
            "uv_index": uv_list[i] if i < len(uv_list) and uv_list[i] is not None else 0.0,
            "pm10":     pm_list[i] if i < len(pm_list) and pm_list[i] is not None else 0.0,
        }

    records = []
    for i, date in enumerate(forecast["dates"]):
        temp_max_c = forecast["temp_max"][i] if forecast["temp_max"][i] is not None else 0.0
        temp_min_c = forecast["temp_min"][i] if forecast["temp_min"][i] is not None else 0.0
        precip     = forecast["precip"][i]   if forecast["precip"][i]   is not None else 0.0
        wind       = forecast["wind"][i]     if forecast["wind"][i]     is not None else 0.0

        # Convert temperature to Fahrenheit for US-based HR team readability
        temp_max_f = round(celsius_to_fahrenheit(temp_max_c), 1)
        temp_min_f = round(celsius_to_fahrenheit(temp_min_c), 1)

        # Look up AQ values for this date; default to 0.0 if AQ data is incomplete
        aq_day = aq_by_date.get(date, {"uv_index": 0.0, "pm10": 0.0})
        uv   = aq_day["uv_index"]
        pm10 = aq_day["pm10"]

        # Apply HR policy risk classification to this single day
        risk = flag_risk_day(temp_max_f, temp_min_f, precip, wind, uv, pm10)

        records.append({
            "City":             city_name,
            "Date":             date,
            "TempMax_F":        temp_max_f,
            "TempMin_F":        temp_min_f,
            "Precipitation_mm": round(precip, 2),
            "Wind_kmh":         round(wind, 1),
            "UV_Index":         round(uv, 2),
            "PM10":             round(pm10, 2),
            "Risk_Flag":        risk,
        })
    return records


# ---------------------------------------------------------------------------
# FETCH FUNCTIONS — each call is independently fault-tolerant
# ---------------------------------------------------------------------------

def _placeholder_forecast():
    """Return 7-day zero-filled forecast used when the API call fails."""
    today = datetime.now(timezone.utc).date()
    dates = [(today + timedelta(days=i)).isoformat() for i in range(7)]
    return {
        "dates":    dates,
        "temp_max": [0.0] * 7,
        "temp_min": [0.0] * 7,
        "precip":   [0.0] * 7,
        "wind":     [0.0] * 7,
        "_placeholder": True,
    }


def _placeholder_air_quality():
    """Return 7-day zero-filled AQ data used when the API call fails."""
    today = datetime.now(timezone.utc).date()
    dates = [(today + timedelta(days=i)).isoformat() for i in range(7)]
    return {
        "dates":    dates,
        "uv_index": [0.0] * 7,
        "pm10":     [0.0] * 7,
        "_placeholder": True,
    }


def fetch_forecast(city_name, lat, lon):
    """
    Fetch 7-day daily weather forecast from Open-Meteo for one city.
    Returns a dict with parallel lists keyed by variable name.
    On any failure, logs the error and returns placeholder data so the pipeline continues.
    """
    params = {
        "latitude":      lat,
        "longitude":     lon,
        "daily":         "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
        "timezone":      "auto",
        "forecast_days": 7,
    }
    try:
        resp = requests.get(FORECAST_URL, params=params, timeout=API_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get("daily", {})
        # Validate all required keys are present before accessing
        required = ["time", "temperature_2m_max", "temperature_2m_min",
                    "precipitation_sum", "wind_speed_10m_max"]
        if not all(k in daily for k in required):
            raise ValueError(f"Missing keys in forecast response for {city_name}")
        print(f"[OK]     Forecast fetched for {city_name}")
        return {
            "dates":    daily["time"],
            "temp_max": daily["temperature_2m_max"],
            "temp_min": daily["temperature_2m_min"],
            "precip":   daily["precipitation_sum"],
            "wind":     daily["wind_speed_10m_max"],
        }
    except Exception as e:
        print(f"[FAILED] Forecast fetch failed for {city_name}: {e}")
        return _placeholder_forecast()


def fetch_air_quality(city_name, lat, lon):
    """
    Fetch hourly UV index and PM10 from Open-Meteo Air Quality API for one city.
    Aggregates hourly → daily MAX values (worst-case exposure for HR risk purposes).
    On any failure, logs the error and returns placeholder data.
    """
    params = {
        "latitude":      lat,
        "longitude":     lon,
        "hourly":        "uv_index,pm10",
        "timezone":      "auto",
        "forecast_days": 7,
    }
    try:
        resp = requests.get(AIR_QUALITY_URL, params=params, timeout=API_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        hourly = data.get("hourly", {})
        if not all(k in hourly for k in ["time", "uv_index", "pm10"]):
            raise ValueError(f"Missing keys in AQ response for {city_name}")

        # Aggregate hourly → daily by grouping on the date portion of the timestamp.
        # We take the daily MAX to represent worst-case exposure for HR risk purposes.
        daily_uv   = {}
        daily_pm10 = {}
        for ts, uv, pm in zip(hourly["time"], hourly["uv_index"], hourly["pm10"]):
            date_key = ts[:10]  # "2026-04-26T13:00" → "2026-04-26"
            uv_val  = uv if uv  is not None else 0.0
            pm_val  = pm if pm  is not None else 0.0
            daily_uv[date_key]   = max(daily_uv.get(date_key, 0.0),   uv_val)
            daily_pm10[date_key] = max(daily_pm10.get(date_key, 0.0), pm_val)

        dates = sorted(daily_uv.keys())
        print(f"[OK]     Air quality fetched for {city_name}")
        return {
            "dates":    dates,
            "uv_index": [daily_uv[d]   for d in dates],
            "pm10":     [daily_pm10[d] for d in dates],
        }
    except Exception as e:
        print(f"[FAILED] Air quality fetch failed for {city_name}: {e}")
        return _placeholder_air_quality()


def fetch_users():
    """
    Fetch simulated new-hire list from JSONPlaceholder (HR system simulation).
    Returns list of user dicts. On failure returns empty list so the pipeline
    produces an empty hire report rather than crashing.
    """
    try:
        resp = requests.get(USERS_URL, timeout=API_TIMEOUT)
        resp.raise_for_status()
        users = resp.json()
        if not isinstance(users, list):
            raise ValueError("Expected list from users API")
        print(f"[OK]     Users fetched: {len(users)} records")
        return users
    except Exception as e:
        print(f"[FAILED] Users fetch failed: {e}")
        return []


# ---------------------------------------------------------------------------
# OUTPUT FUNCTIONS — write results to disk
# ---------------------------------------------------------------------------

def write_weather_csv(all_day_records, filepath):
    """
    Write all 35 day records (5 cities × 7 days) to CSV.
    One row per city-day with all weather metrics and risk classification.
    """
    fieldnames = ["City", "Date", "TempMax_F", "TempMin_F",
                  "Precipitation_mm", "Wind_kmh", "UV_Index", "PM10", "Risk_Flag"]
    try:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_day_records)
        print(f"[OK]     Weather CSV written: {filepath} ({len(all_day_records)} rows)")
    except Exception as e:
        print(f"[FAILED] Could not write weather CSV: {e}")


def write_hire_csv(hire_rows, filepath):
    """
    Write one row per new hire with city assignment and onboarding recommendation.
    HR uses this to send personalized onboarding instructions to each employee.
    """
    fieldnames = ["Employee_Name", "Employee_Email", "Assigned_City",
                  "City_Risk_Score", "Onboarding_Recommendation"]
    try:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(hire_rows)
        print(f"[OK]     Hire report CSV written: {filepath} ({len(hire_rows)} rows)")
    except Exception as e:
        print(f"[FAILED] Could not write hire report CSV: {e}")


def generate_html_report(city_stats_list, hire_rows, fetched_at, filepath):
    """
    Generate a professional inline-CSS HTML report for HR leadership.
    No external stylesheets or JS — fully self-contained for email attachment.

    Sections: header, executive summary, city risk rankings,
              new hire recommendations, data pipeline, ML enhancements.
    """
    today_str   = datetime.now().strftime("%B %d, %Y")
    fetched_str = fetched_at.strftime("%Y-%m-%d %H:%M UTC")

    # Sort cities safest -> riskiest for the rankings table
    ranked = sorted(city_stats_list, key=lambda c: c["risk_score"])

    # Identify key findings for the auto-generated executive summary
    safest        = ranked[0]
    riskiest      = ranked[-1]
    remote_count  = sum(1 for h in hire_rows if h["Onboarding_Recommendation"] == "REMOTE")
    high_var_city = max(city_stats_list, key=lambda c: c["temp_variance"])

    summary = (
        f"Analysis of 7-day weather forecasts across all 5 office locations reveals that "
        f"<strong>{riskiest['city']}</strong> carries the highest environmental risk score "
        f"({riskiest['risk_score']:.1f}%), while <strong>{safest['city']}</strong> is the "
        f"safest option for in-person onboarding ({safest['risk_score']:.1f}% risk). "
        f"A total of <strong>{remote_count} out of {len(hire_rows)}</strong> new hires are "
        f"recommended for remote onboarding due to their assigned city exceeding the 40% "
        f"high-risk threshold. "
        f"<strong>{high_var_city['city']}</strong> exhibits the highest temperature variance "
        f"({high_var_city['temp_variance']:.1f}°F range), requiring careful dress-code "
        f"guidance for any in-person sessions scheduled there."
    )

    def risk_badge(score):
        if score > RISK_SCORE_THRESHOLD:
            return ('<span style="background:#e74c3c;color:#fff;padding:2px 8px;'
                    'border-radius:4px;font-size:12px;">HIGH</span>')
        return ('<span style="background:#27ae60;color:#fff;padding:2px 8px;'
                'border-radius:4px;font-size:12px;">LOW</span>')

    # Build city rankings table rows (alternating row colors)
    city_rows_html = ""
    for idx, city in enumerate(ranked):
        bg = "#f9f9f9" if idx % 2 == 0 else "#ffffff"
        city_rows_html += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:8px 12px;">{idx + 1}</td>'
            f'<td style="padding:8px 12px;font-weight:bold;">{html.escape(city["city"])}</td>'
            f'<td style="padding:8px 12px;">{city["risk_score"]:.1f}%</td>'
            f'<td style="padding:8px 12px;">{risk_badge(city["risk_score"])}</td>'
            f'<td style="padding:8px 12px;">{city["avg_precip"]:.1f} mm</td>'
            f'<td style="padding:8px 12px;">{city["avg_uv"]:.1f}</td>'
            f'<td style="padding:8px 12px;">{city["temp_variance"]:.1f}°F</td>'
            f'</tr>'
        )

    # Build hire recommendations table rows (alternating row colors)
    hire_rows_html = ""
    for idx, hire in enumerate(hire_rows):
        bg  = "#f9f9f9" if idx % 2 == 0 else "#ffffff"
        rec = hire["Onboarding_Recommendation"]
        rec_color = "#e74c3c" if rec == "REMOTE" else "#27ae60"
        hire_rows_html += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:8px 12px;">{html.escape(hire["Employee_Name"])}</td>'
            f'<td style="padding:8px 12px;">{html.escape(hire["Employee_Email"])}</td>'
            f'<td style="padding:8px 12px;">{html.escape(hire["Assigned_City"])}</td>'
            f'<td style="padding:8px 12px;">{hire["City_Risk_Score"]:.1f}%</td>'
            f'<td style="padding:8px 12px;font-weight:bold;color:{rec_color};">{rec}</td>'
            f'</tr>'
        )

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>HR Onboarding Scheduling Risk Report</title>
</head>
<body style="font-family:Arial,sans-serif;max-width:960px;margin:0 auto;padding:20px;color:#333;">

  <div style="background:#2c3e50;color:#fff;padding:24px 32px;border-radius:8px;margin-bottom:24px;">
    <h1 style="margin:0;font-size:24px;">HR Onboarding Scheduling Risk Report</h1>
    <p style="margin:8px 0 0;opacity:0.8;">{today_str} &nbsp;|&nbsp; Data fetched at: {fetched_str}</p>
  </div>

  <div style="background:#eaf4fb;border-left:4px solid #2980b9;padding:16px 20px;margin-bottom:24px;border-radius:4px;">
    <h2 style="margin:0 0 8px;font-size:18px;color:#2980b9;">Executive Summary</h2>
    <p style="margin:0;line-height:1.6;">{summary}</p>
  </div>

  <div style="margin-bottom:24px;">
    <h2 style="font-size:18px;border-bottom:2px solid #eee;padding-bottom:8px;">City Risk Rankings</h2>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <thead>
        <tr style="background:#2c3e50;color:#fff;">
          <th style="padding:10px 12px;text-align:left;">#</th>
          <th style="padding:10px 12px;text-align:left;">City</th>
          <th style="padding:10px 12px;text-align:left;">Risk Score</th>
          <th style="padding:10px 12px;text-align:left;">Level</th>
          <th style="padding:10px 12px;text-align:left;">Avg Precip</th>
          <th style="padding:10px 12px;text-align:left;">Avg UV</th>
          <th style="padding:10px 12px;text-align:left;">Temp Variance</th>
        </tr>
      </thead>
      <tbody>{city_rows_html}</tbody>
    </table>
  </div>

  <div style="margin-bottom:24px;">
    <h2 style="font-size:18px;border-bottom:2px solid #eee;padding-bottom:8px;">New Hire Onboarding Recommendations</h2>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <thead>
        <tr style="background:#2c3e50;color:#fff;">
          <th style="padding:10px 12px;text-align:left;">Employee Name</th>
          <th style="padding:10px 12px;text-align:left;">Email</th>
          <th style="padding:10px 12px;text-align:left;">Assigned City</th>
          <th style="padding:10px 12px;text-align:left;">City Risk Score</th>
          <th style="padding:10px 12px;text-align:left;">Recommendation</th>
        </tr>
      </thead>
      <tbody>{hire_rows_html}</tbody>
    </table>
  </div>

  <div style="background:#f8f9fa;padding:16px 20px;margin-bottom:24px;border-radius:4px;font-size:14px;">
    <h2 style="font-size:18px;margin-top:0;">Data Pipeline</h2>
    <p><strong>Source 1 — Open-Meteo Forecast API:</strong> 7-day daily weather for 5 cities
    (temperature max/min in °C converted to °F, precipitation in mm, wind speed in km/h).
    Temperatures converted using standard formula (°C × 9/5 + 32).</p>
    <p><strong>Source 2 — Open-Meteo Air Quality API:</strong> Hourly UV index and PM10 particulate
    readings aggregated to daily maximums for worst-case risk assessment.</p>
    <p><strong>Source 3 — JSONPlaceholder Users API:</strong> 10 simulated new hire records assigned
    to cities in round-robin order (2 per city). Each hire's city risk score drives the
    IN-PERSON vs REMOTE recommendation using a 40% high-risk-day threshold per HR policy.</p>
    <p><strong>Transformations:</strong> Each day flagged HIGH RISK if any threshold exceeded
    (temp &lt; 20°F, temp &gt; 95°F, precip &gt; 20mm, wind &gt; 50 km/h, UV &gt; 8, PM10 &gt; 50).
    City risk score = % of 7 forecast days classified HIGH RISK.</p>
  </div>

  <div style="background:#f0f8e8;padding:16px 20px;border-radius:4px;font-size:14px;">
    <h2 style="font-size:18px;margin-top:0;">ML Enhancement Opportunities</h2>
    <ul style="line-height:1.8;margin:0;padding-left:20px;">
      <li><strong>Anomaly Detection:</strong> Apply Isolation Forest or Z-score analysis on
      rolling 30-day weather history to flag cities exhibiting statistically unusual conditions
      (e.g., unseasonable heat waves or AQ spikes) not caught by static thresholds alone.</li>
      <li><strong>Predictive Scheduling:</strong> Train a gradient boosted model on historical
      NOAA weather data to forecast 14–30 day risk windows, enabling HR to proactively schedule
      onboarding cohorts during climatically favorable periods rather than reacting week-by-week.</li>
      <li><strong>NLP Preference Classification:</strong> Apply a fine-tuned BERT classifier to
      employee onboarding survey responses to extract location preferences and commute tolerance,
      then weight each hire's recommendation by combining weather risk score with their personal
      preference signal for a fully personalized hybrid-scheduling output.</li>
    </ul>
  </div>

</body>
</html>"""

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"[OK]     HTML report written: {filepath}")
    except Exception as e:
        print(f"[FAILED] Could not write HTML report: {e}")


def main():
    """
    Orchestrate the full RPA pipeline:
      1. Fetch data from all 3 API sources
      2. Build per-day records and compute city-level stats
      3. Build hire report with IN-PERSON/REMOTE recommendations
      4. Write all 3 output files
      5. Print a console summary for quick verification
    """
    import os
    print("=" * 60)
    print("HR Onboarding Weather Bot — Starting pipeline")
    print("=" * 60)

    fetched_at = datetime.now(timezone.utc)
    city_names = list(CITIES.keys())

    # --- Fetch and transform all city data ---
    all_day_records = []
    city_stats_list = []

    for city_name, coords in CITIES.items():
        forecast    = fetch_forecast(city_name, coords["lat"], coords["lon"])
        aq          = fetch_air_quality(city_name, coords["lat"], coords["lon"])
        day_records = build_day_records(city_name, forecast, aq)
        all_day_records.extend(day_records)
        city_stats_list.append(compute_city_stats(city_name, day_records))

    # Fetch simulated new-hire list from HR system (JSONPlaceholder)
    users = fetch_users()

    # --- Build hire report ---
    assignments  = assign_users_to_cities(users, city_names)
    risk_by_city = {s["city"]: s["risk_score"] for s in city_stats_list}

    hire_rows = []
    for a in assignments:
        city_score = risk_by_city.get(a["Assigned_City"], 0.0)
        hire_rows.append({
            "Employee_Name":             a["Employee_Name"],
            "Employee_Email":            a["Employee_Email"],
            "Assigned_City":             a["Assigned_City"],
            "City_Risk_Score":           city_score,
            "Onboarding_Recommendation": recommend_onboarding(city_score),
        })

    # --- Write all 3 output files ---
    script_dir       = os.path.dirname(os.path.abspath(__file__))
    weather_csv_path = os.path.join(script_dir, "onboarding_weather_data.csv")
    hire_csv_path    = os.path.join(script_dir, "onboarding_hire_report.csv")
    html_path        = os.path.join(script_dir, "onboarding_report.html")

    write_weather_csv(all_day_records, weather_csv_path)
    write_hire_csv(hire_rows, hire_csv_path)
    generate_html_report(city_stats_list, hire_rows, fetched_at, html_path)

    # --- Console summary ---
    print()
    print("=" * 60)
    print("CITY RISK RANKINGS (safest -> riskiest)")
    print("-" * 60)
    ranked = sorted(city_stats_list, key=lambda c: c["risk_score"])
    for i, city in enumerate(ranked, 1):
        level = "HIGH" if city["risk_score"] > RISK_SCORE_THRESHOLD else "LOW "
        print(f"  {i}. {city['city']:<15} risk={city['risk_score']:5.1f}%  [{level}]  "
              f"avg_precip={city['avg_precip']:5.1f}mm  avg_uv={city['avg_uv']:4.1f}  "
              f"temp_variance={city['temp_variance']:5.1f}°F")

    print()
    print("NEW HIRE ONBOARDING RECOMMENDATIONS")
    print("-" * 60)
    for h in hire_rows:
        print(f"  {h['Employee_Name']:<25} -> {h['Assigned_City']:<15} "
              f"[{h['Onboarding_Recommendation']}]")

    remote_count   = sum(1 for h in hire_rows if h["Onboarding_Recommendation"] == "REMOTE")
    inperson_count = len(hire_rows) - remote_count
    print()
    print(f"  Summary: {inperson_count} IN-PERSON  |  {remote_count} REMOTE")
    print("=" * 60)
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
