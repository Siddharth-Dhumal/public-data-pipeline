from __future__ import annotations
from dataclasses import dataclass  
from datetime import datetime, timezone
from typing import Iterable, List, Tuple
from sqlalchemy import (
	Column, Integer, Float, String,
	DateTime, UniqueConstraint, Index,
)
from sqlalchemy.orm import Mapped, mapped_column
from pipeline.db import Base
from sqlalchemy.dialects.mysql import DATETIME as MySQLDateTime

def _utc_naive(dt: datetime) -> datetime:
	if dt.tzinfo is None:
		return dt  
	return dt.astimezone(timezone.utc).replace(tzinfo=None)

class WeatherHourlyORM(Base):
	__tablename__ = "weather_hourly"
	id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
	ts_utc: Mapped[datetime] = mapped_column(MySQLDateTime(fsp=6), nullable=False)
	temperature_c: Mapped[float | None] = mapped_column(Float(asdecimal=False), nullable=True)
	windspeed_mps: Mapped[float | None] = mapped_column(Float(asdecimal=False), nullable=True)
	precipitation_mm: Mapped[float | None] = mapped_column(Float(asdecimal=False), nullable=True)
	lat: Mapped[float] = mapped_column(Float(asdecimal=False), nullable=False)
	lon: Mapped[float] = mapped_column(Float(asdecimal=False), nullable=False)
	source: Mapped[str] = mapped_column(String(32), nullable=False, default="open-meteo")
	__table_args__ = (
		UniqueConstraint("ts_utc", "lat", "lon", "source", name="uq_weather_ts_utc_lat_lon_source"),
		Index("ix_weather_ts", "ts_utc"),
		Index("ix_weather_lat_lon", "lat", "lon"),
	)

class EarthquakeORM(Base):
	__tablename__ = "earthquakes"
	id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
	event_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
	ts_utc: Mapped[datetime] = mapped_column(MySQLDateTime(fsp=6), nullable=False)
	magnitude: Mapped[float | None] = mapped_column(Float(asdecimal=False), nullable=True)
	depth_km: Mapped[float | None] = mapped_column(Float(asdecimal=False), nullable=True)
	place: Mapped[str | None] = mapped_column(String(255), nullable=True)
	lat: Mapped[float | None] = mapped_column(Float(asdecimal=False), nullable=True)
	lon: Mapped[float | None] = mapped_column(Float(asdecimal=False), nullable=True)
	__table_args__ = (
		Index("ix_quakes_ts", "ts_utc"),
		Index("ix_quakes_magnitude", "magnitude"),
	)

def weather_rows(samples: Iterable["WeatherSample"]) -> List[dict]:
	from pipeline.domain.models import WeatherSample
	rows: List[dict] = []
	for s in samples:
		assert isinstance(s, WeatherSample)
		rows.append(
			dict(
				ts_utc=_utc_naive(s.ts_utc),
				temperature_c=s.temperature_c,
				windspeed_mps=s.windspeed_mps,
				precipitation_mm=s.precipitation_mm,
				lat=s.lat,
				lon=s.lon,
				source=s.source,
			)
		)
	return rows

def quake_rows(events: Iterable["EarthquakeEvent"]) -> List[dict]:
	from pipeline.domain.models import EarthquakeEvent
	rows: List[dict] = []
	for e in events:
		assert isinstance(e, EarthquakeEvent)
		rows.append(
			dict(
				event_id=e.event_id,
				ts_utc=_utc_naive(e.ts_utc),
				magnitude=e.magnitude,
				depth_km=e.depth_km,
				place=e.place,
				lat=e.lat,
				lon=e.lon,
			)
		)
	return rows