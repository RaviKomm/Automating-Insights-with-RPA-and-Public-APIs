# HR Onboarding Scheduling Risk Bot
### Week 6 Assignment — Automating Insights with RPA and Public APIs

---

## Business Scenario

An HR department manages new-hire onboarding sessions across **5 US office locations**: Minneapolis, Chicago, New York, Austin, and Denver. Every week, HR needs to decide whether incoming employees should attend onboarding **in person** or **remotely** based on environmental conditions at each location.

This Python RPA bot automates that decision entirely. It runs once, fetches live weather and air quality data from public APIs, scores each city's risk level for the upcoming 7 days, cross-references the new hire list, and produces ready-to-use reports — all without any manual input or API keys.

---

## What the Bot Does

```
[Fetch Layer]  →  [Transform Layer]  →  [Output Layer]
  3 API calls      risk scoring           3 report files
```

**Step 1 — Fetch** live data from 3 sources:
- 7-day weather forecast per city (temperature, precipitation, wind)
- 7-day air quality forecast per city (UV index, PM10 particulates)
- Simulated new-hire list from the HR system

**Step 2 — Transform** the raw data into decisions:
- Convert temperatures from Celsius to Fahrenheit
- Flag each day as HIGH RISK or LOW RISK using 6 safety thresholds
- Calculate a 7-day risk score (% of high-risk days) per city
- Rank all 5 cities from safest to riskiest
- Assign new hires to cities and recommend IN-PERSON or REMOTE onboarding

**Step 3 — Output** three formatted reports automatically

---

## Data Sources

| Source | API | What It Provides |
|--------|-----|-----------------|
| Open-Meteo Forecast | `api.open-meteo.com/v1/forecast` | Daily temp max/min, precipitation, wind speed |
| Open-Meteo Air Quality | `air-quality-api.open-meteo.com/v1/air-quality` | Hourly UV index and PM10 — aggregated to daily max |
| JSONPlaceholder Users | `jsonplaceholder.typicode.com/users` | Simulated new-hire employee records |

> No API keys required. All three sources are free and publicly accessible.

---

## Risk Thresholds (Business Logic)

A day is flagged **HIGH RISK** if **any one** of these conditions is met:

| Condition | Threshold | Reason |
|-----------|-----------|--------|
| Temperature too cold | Below 20°F | Dangerous commute conditions |
| Temperature too hot | Above 95°F | Heat safety risk |
| Heavy precipitation | Over 20 mm | Flooding / travel disruption |
| High wind speed | Over 50 km/h | Travel hazard |
| High UV index | Above 8 | Prolonged outdoor exposure risk |
| Poor air quality (PM10) | Above 50 µg/m³ | Health risk for sensitive individuals |

A city's **Risk Score** = percentage of the 7-day forecast that is HIGH RISK.  
Cities with a risk score **above 40%** trigger a **REMOTE** onboarding recommendation.

---

## Output Files

### 1. `onboarding_weather_data.csv`
35 rows — one per city per forecast day (5 cities × 7 days).

| Column | Description |
|--------|-------------|
| City | Office location |
| Date | Forecast date |
| TempMax_F | Daily high in Fahrenheit |
| TempMin_F | Daily low in Fahrenheit |
| Precipitation_mm | Daily rainfall in millimetres |
| Wind_kmh | Max wind speed in km/h |
| UV_Index | Peak daily UV index |
| PM10 | Peak daily particulate matter (µg/m³) |
| Risk_Flag | HIGH RISK or LOW RISK |

---

### 2. `onboarding_hire_report.csv`
10 rows — one per new hire.

| Column | Description |
|--------|-------------|
| Employee_Name | New hire full name |
| Employee_Email | Contact email |
| Assigned_City | Office location assigned via round-robin |
| City_Risk_Score | 7-day risk % for their city |
| Onboarding_Recommendation | **IN-PERSON** or **REMOTE** |

---

### 3. `onboarding_report.html`
A fully self-contained executive report with inline CSS (no dependencies). Open in any browser.

Sections:
- **Header** — Report date and data freshness timestamp
- **Executive Summary** — Auto-generated 3–4 sentence narrative: safest city, riskiest city, remote count, highest temperature variance city
- **City Risk Rankings** — Sorted table with colour-coded LOW (green) / HIGH (red) badges
- **New Hire Recommendations** — Per-employee onboarding decision table
- **Data Pipeline** — Plain-English description of the three sources and transformations
- **ML Enhancement Opportunities** — Three ways machine learning could extend this automation

---

## How to Run

**Requirements:** Python 3.x and the `requests` library.

```bash
pip install requests
python onboarding_weather_bot.py
```

The script prints a live status line for every API call and a full console summary on completion. The three output files are written to the same folder as the script.

**Sample console output:**
```
============================================================
HR Onboarding Weather Bot — Starting pipeline
============================================================
[OK]     Forecast fetched for Minneapolis
[OK]     Air quality fetched for Minneapolis
...
[OK]     Users fetched: 10 records
[OK]     Weather CSV written: onboarding_weather_data.csv (35 rows)
[OK]     Hire report CSV written: onboarding_hire_report.csv (10 rows)
[OK]     HTML report written: onboarding_report.html

CITY RISK RANKINGS (safest -> riskiest)
------------------------------------------------------------
  1. Minneapolis     risk=  0.0%  [LOW ]  ...
  5. Denver          risk= 14.3%  [LOW ]  ...
============================================================
```

---

## Error Handling

Every API call is independently fault-tolerant:

- **10-second timeout** on every request
- **Key validation** — checks that the response contains expected fields before accessing them
- **Placeholder fallback** — if any call fails, the pipeline continues with safe zero-filled values rather than crashing
- **[OK] / [FAILED] status** printed to the console for every call so failures are immediately visible

This means the bot will always produce output, even if one or more APIs are temporarily unavailable.

---

## Test Suite

32 unit tests covering all business logic — run with:

```bash
python -m pytest tests/ -v
```

| Test File | What It Covers |
|-----------|---------------|
| `tests/test_transforms.py` | Unit conversion, risk flagging, city stats, user assignment, recommendations, data merging |
| `tests/test_fetch.py` | API success paths, failure fallbacks, missing-key handling |

---

## ML Enhancement Opportunities

The bot currently uses fixed rule-based thresholds. Three ways machine learning could improve it:

1. **Anomaly Detection** — Apply Isolation Forest or Z-score analysis on rolling 30-day weather history to catch unusual conditions (e.g., an unseasonable heat wave) that static thresholds would miss.

2. **Predictive Scheduling** — Train a model on historical NOAA weather data to forecast 14–30 day risk windows, allowing HR to plan onboarding cohorts weeks in advance rather than reacting to the 7-day forecast.

3. **NLP Preference Classification** — Apply a text classifier to employee onboarding survey responses to extract location preferences and commute tolerance, then combine that signal with the weather risk score for a fully personalized recommendation.

---

## Project Structure

```
week 6/
├── onboarding_weather_bot.py       Main RPA script
├── onboarding_weather_data.csv     Generated output — weather data
├── onboarding_hire_report.csv      Generated output — hire decisions
├── onboarding_report.html          Generated output — HTML executive report
├── tests/
│   ├── test_transforms.py          Unit tests — business logic
│   └── test_fetch.py               Unit tests — API fetch layer
└── README.md                       This file
```

---

## Technologies Used

| Library | Purpose |
|---------|---------|
| `requests` | HTTP calls to all three APIs |
| `csv` | Reading and writing CSV files |
| `html` | HTML escaping for safe report generation |
| `datetime` | Date arithmetic for 7-day forecast windows |
| `statistics` | Mean calculations for city averages |

Standard library only (except `requests`) — no pandas, no external dependencies beyond one install.
