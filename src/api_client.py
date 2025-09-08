\
import os
import datetime as dt
import requests
import pandas as pd
from loguru import logger

def _session(timeout: int = 15):
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    s.headers.update({
        "User-Agent": "Aula04-Coleta/1.0 (+https://example.com)",
        "Accept": "application/json",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    })
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        redirect=3,
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=0.5,
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.request_timeout = timeout
    return s

def geocode_city(city: str, language: str = "pt"):
    url = "https://geocoding-api.open-meteo.com/v1/search"
    s = _session()
    r = s.get(url, params={"name": city, "count": 1, "language": language, "format": "json"}, timeout=s.request_timeout)
    r.raise_for_status()
    js = r.json()
    if not js.get("results"):
        raise ValueError(f"Cidade não encontrada: {city}")
    res = js["results"][0]
    return {"name": res["name"], "latitude": res["latitude"], "longitude": res["longitude"], "country": res.get("country", "")}

def fetch_weather(lat: float, lon: float, tz: str = "UTC"):
    url = "https://api.open-meteo.com/v1/forecast"
    s = _session()
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m",
        "timezone": tz,
        "past_days": 1,
        "forecast_days": 1,
    }
    r = s.get(url, params=params, timeout=s.request_timeout)
    r.raise_for_status()
    return r.json()

def normalize_hourly(city: str, weather_js: dict) -> pd.DataFrame:
    h = weather_js.get("hourly", {})
    df = pd.DataFrame(h)
    if df.empty:
        return df
    df.insert(0, "city", city)
    return df

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    CITY = os.getenv("CITY", "Goiânia")
    LANGUAGE = os.getenv("LANGUAGE", "pt")
    TIMEZONE = os.getenv("TIMEZONE", "UTC")

    loc = geocode_city(CITY, language=LANGUAGE)
    js = fetch_weather(loc["latitude"], loc["longitude"], tz=TIMEZONE)
    df = normalize_hourly(CITY, js)

    out = os.path.join(os.path.dirname(__file__), "..", "data", f"weather_hourly_{CITY}_{dt.date.today().isoformat()}.csv")
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"[OK] Meteo salvo em: {out}")
