import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from unittest.mock import patch, MagicMock
from onboarding_weather_bot import fetch_forecast, fetch_air_quality, fetch_users


# --- fetch_forecast ---

def test_fetch_forecast_success():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "daily": {
            "time":               ["2026-04-26", "2026-04-27"],
            "temperature_2m_max": [20.0, 22.0],
            "temperature_2m_min": [10.0, 12.0],
            "precipitation_sum":  [0.0,  5.0],
            "wind_speed_10m_max": [15.0, 20.0],
        }
    }
    mock_resp.raise_for_status = MagicMock()
    with patch("onboarding_weather_bot.requests.get", return_value=mock_resp):
        result = fetch_forecast("TestCity", 44.98, -93.27)
    assert result["dates"] == ["2026-04-26", "2026-04-27"]
    assert result["temp_max"] == [20.0, 22.0]
    assert result["temp_min"] == [10.0, 12.0]
    assert result["precip"] == [0.0, 5.0]
    assert result["wind"] == [15.0, 20.0]
    mock_resp.raise_for_status.assert_called_once()

def test_fetch_forecast_failure_returns_placeholder():
    with patch("onboarding_weather_bot.requests.get", side_effect=Exception("timeout")):
        result = fetch_forecast("TestCity", 44.98, -93.27)
    assert "dates" in result
    assert len(result["dates"]) == 7  # placeholder must have 7 days


# --- fetch_air_quality ---

def test_fetch_air_quality_success():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "hourly": {
            "time":     ["2026-04-26T00:00", "2026-04-26T01:00", "2026-04-27T00:00"],
            "uv_index": [0.0, 0.5, 3.0],
            "pm10":     [12.0, 14.0, 18.0],
        }
    }
    mock_resp.raise_for_status = MagicMock()
    with patch("onboarding_weather_bot.requests.get", return_value=mock_resp):
        result = fetch_air_quality("TestCity", 44.98, -93.27)
    assert "dates" in result
    assert "uv_index" in result
    assert "pm10" in result
    assert len(result["dates"]) == 2          # 2 distinct calendar dates
    assert result["uv_index"][0] == 0.5       # daily max of [0.0, 0.5]
    assert result["pm10"][0] == 14.0          # daily max of [12.0, 14.0]
    assert result["pm10"][1] == 18.0
    mock_resp.raise_for_status.assert_called_once()

def test_fetch_air_quality_failure_returns_placeholder():
    with patch("onboarding_weather_bot.requests.get", side_effect=Exception("timeout")):
        result = fetch_air_quality("TestCity", 44.98, -93.27)
    assert "dates" in result
    assert len(result["dates"]) == 7


# --- fetch_users ---

def test_fetch_users_success():
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"name": "Alice", "email": "alice@test.com"},
        {"name": "Bob",   "email": "bob@test.com"},
    ]
    mock_resp.raise_for_status = MagicMock()
    with patch("onboarding_weather_bot.requests.get", return_value=mock_resp):
        result = fetch_users()
    assert len(result) == 2
    assert result[0]["name"] == "Alice"

def test_fetch_forecast_missing_keys_returns_placeholder():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"daily": {"time": ["2026-04-26"]}}  # missing keys
    mock_resp.raise_for_status = MagicMock()
    with patch("onboarding_weather_bot.requests.get", return_value=mock_resp):
        result = fetch_forecast("TestCity", 44.98, -93.27)
    assert result.get("_placeholder") is True

def test_fetch_air_quality_missing_keys_returns_placeholder():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"hourly": {"time": ["2026-04-26T00:00"]}}  # missing keys
    mock_resp.raise_for_status = MagicMock()
    with patch("onboarding_weather_bot.requests.get", return_value=mock_resp):
        result = fetch_air_quality("TestCity", 44.98, -93.27)
    assert result.get("_placeholder") is True

def test_fetch_users_failure_returns_empty_list():
    with patch("onboarding_weather_bot.requests.get", side_effect=Exception("timeout")):
        result = fetch_users()
    assert isinstance(result, list)
    assert len(result) == 0
