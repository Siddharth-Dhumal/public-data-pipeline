from __future__ import annotations
from typing import Iterable
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session
from pipeline.adapters.repositories.orm import WeatherHourlyORM, weather_rows
from pipeline.db import safe_session
from pipeline.domain.models import WeatherSample
from pipeline.ports.repositories import WeatherRepository

class MySQLWeatherRepository(WeatherRepository):
	def __init__(self, session: Session | None = None) -> None:
		self._session = session

	def upsert_many(self, samples: Iterable["WeatherSample"]) -> int:
		rows = weather_rows(samples)
		if not rows:
			return 0

		def _upsert(s: Session) -> int:
			stmt = mysql_insert(WeatherHourlyORM).values(rows)
			update_cols = {
				"temperature_c": stmt.inserted.temperature_c,
				"windspeed_mps": stmt.inserted.windspeed_mps,
				"precipitation_mm": stmt.inserted.precipitation_mm,
			}
			ondup = stmt.on_duplicate_key_update(**update_cols)
			res = s.execute(ondup)
			return len(rows)

		if self._session is not None:
			return _upsert(self._session)

		with safe_session() as s:
			n = _upsert(s)
			s.commit()
			return n