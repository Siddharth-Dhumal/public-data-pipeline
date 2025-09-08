from __future__ import annotations
from datetime import datetime, timezone
from typing import Iterable, List, Optional
import requests
from pipeline.config import settings
from pipeline.domain.models import WeatherSample
from pipeline.ports.extractors import WeatherExtractor

class OpenMeteoClient(WeatherExtractor):
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    def __init__(
        self,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        timeout_s: Optional[int] = None,
    ) -> None:
        self._lat = settings.open_meteo_lat if lat is None else float(lat)
        self._lon = settings.open_meteo_lon if lon is None else float(lon)
        self._timeout = settings.request_timeout_s if timeout_s is None else int(timeout_s)

    def fetch(self) -> Iterable[WeatherSample]:
        params = {
            "latitude": self._lat,
            "longitude": self._lon,
            "hourly": "temperature_2m,precipitation,wind_speed_10m",
            "timezone": "UTC",
        }
        r = requests.get(self.BASE_URL, params=params, timeout=self._timeout)
        r.raise_for_status()
        data = r.json()

        hourly = data.get("hourly") or {}
        times: List[str] = hourly.get("time") or []
        temps: List[Optional[float]] = hourly.get("temperature_2m") or []
        precs: List[Optional[float]] = hourly.get("precipitation") or []
        winds: List[Optional[float]] = hourly.get("wind_speed_10m") or []

        n = min(len(times), len(temps), len(precs), len(winds))
        for i in range(n):
            ts_str = times[i]
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)

            yield WeatherSample(
                ts_utc=ts,
                temperature_c=temps[i],
                windspeed_mps=winds[i],
                precipitation_mm=precs[i],
                lat=self._lat,
                lon=self._lon,
                source="open-meteo",
            )