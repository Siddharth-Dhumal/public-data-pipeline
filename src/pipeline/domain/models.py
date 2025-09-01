from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_validator

def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

class WeatherSample(BaseModel):
    ts_utc: datetime = Field(..., description="Timestamp in UTC")
    temperature_c: Optional[float] = Field(default=None, description="Air temperature (°C)")
    windspeed_mps: Optional[float] = Field(default=None, description="Wind speed (m/s)")
    precipitation_mm: Optional[float] = Field(default=None, description="Precipitation (mm)")
    lat: float = Field(..., description="Latitude of the location")
    lon: float = Field(..., description="Longitude of the location")
    source: str = Field(default="open-meteo", description="Origin of the reading")

    @field_validator("ts_utc")
    @classmethod
    def _normalize_ts(cls, v: datetime) -> datetime:
        return _ensure_utc(v)

    @field_validator("lat")
    @classmethod
    def _validate_lat(cls, v: float) -> float:
        if not (-90.0 <= float(v) <= 90.0):
            raise ValueError("lat must be between -90 and 90")
        return float(v)

    @field_validator("lon")
    @classmethod
    def _validate_lon(cls, v: float) -> float:
        if not (-180.0 <= float(v) <= 180.0):
            raise ValueError("lon must be between -180 and 180")
        return float(v)

    @classmethod
    def from_open_meteo_row(
        cls,
        *,
        ts: datetime,
        temperature_c: Optional[float],
        windspeed_mps: Optional[float],
        precipitation_mm: Optional[float],
        lat: float,
        lon: float,
    ) -> "WeatherSample":
        return cls(
            ts_utc=_ensure_utc(ts),
            temperature_c=None if temperature_c is None else float(temperature_c),
            windspeed_mps=None if windspeed_mps is None else float(windspeed_mps),
            precipitation_mm=None if precipitation_mm is None else float(precipitation_mm),
            lat=float(lat),
            lon=float(lon),
            source="open-meteo",
        )


class EarthquakeEvent(BaseModel):
    event_id: str = Field(..., description="Stable external identifier")
    ts_utc: datetime = Field(..., description="Event origin time in UTC")
    magnitude: Optional[float] = Field(default=None, description="Magnitude (Mw)")
    depth_km: Optional[float] = Field(default=None, description="Depth in kilometers")
    place: Optional[str] = Field(default=None, description="Human-readable location")
    lat: Optional[float] = Field(default=None, description="Epicenter latitude")
    lon: Optional[float] = Field(default=None, description="Epicenter longitude")
    source: str = Field(default="usgs", description="Origin of the record")

    @field_validator("ts_utc")
    @classmethod
    def _normalize_ts(cls, v: datetime) -> datetime:
        return _ensure_utc(v)

    @field_validator("lat")
    @classmethod
    def _validate_lat(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        v = float(v)
        if not (-90.0 <= v <= 90.0):
            raise ValueError("lat must be between -90 and 90")
        return v

    @field_validator("lon")
    @classmethod
    def _validate_lon(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        v = float(v)
        if not (-180.0 <= v <= 180.0):
            raise ValueError("lon must be between -180 and 180")
        return v

    @classmethod
    def from_usgs_feature(
        cls,
        *,
        event_id: str,
        ts_ms: int,
        magnitude: Optional[float],
        depth_km: Optional[float],
        place: Optional[str],
        lat: Optional[float],
        lon: Optional[float],
    ) -> "EarthquakeEvent":
        ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return cls(
            event_id=event_id,
            ts_utc=ts,
            magnitude=None if magnitude is None else float(magnitude),
            depth_km=None if depth_km is None else float(depth_km),
            place=place,
            lat=lat if lat is None else float(lat),
            lon=lon if lon is None else float(lon),
            source="usgs",
        )