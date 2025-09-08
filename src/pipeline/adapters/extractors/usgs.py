from __future__ import annotations
from typing import Iterable, Optional
import requests
from pipeline.domain.models import EarthquakeEvent
from pipeline.ports.extractors import EarthquakeExtractor

class USGSClient(EarthquakeExtractor):
    FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    def __init__(self, timeout_s: Optional[int] = None) -> None:
        from pipeline.config import settings  # local import to avoid cycles
        self._timeout = settings.request_timeout_s if timeout_s is None else int(timeout_s)

    def fetch(self) -> Iterable[EarthquakeEvent]:
        r = requests.get(self.FEED_URL, timeout=self._timeout)
        r.raise_for_status()
        data = r.json()

        for feature in data.get("features", []):
            props = feature.get("properties", {}) or {}
            geom = feature.get("geometry", {}) or {}
            coords = geom.get("coordinates") or [None, None, None]  # [lon, lat, depth_km]
            lon, lat, depth_km = (coords + [None, None, None])[:3]

            yield EarthquakeEvent.from_usgs_feature(
                event_id=feature.get("id") or props.get("code") or "unknown",
                ts_ms=props.get("time"),
                magnitude=props.get("mag"),
                depth_km=depth_km,
                place=props.get("place"),
                lat=lat,
                lon=lon,
            )