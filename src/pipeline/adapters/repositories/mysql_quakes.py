from __future__ import annotations
from typing import Iterable
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session
from pipeline.adapters.repositories.orm import EarthquakeORM, quake_rows
from pipeline.db import safe_session
from pipeline.domain.models import EarthquakeEvent
from pipeline.ports.repositories import EarthquakeRepository

class MySQLEarthquakeRepository(EarthquakeRepository):
	def __init__(self, session: Session | None = None) -> None:
		self._session = session

	def upsert_many(self, events: Iterable["EarthquakeEvent"]):
		rows = quake_rows(events)
		if not rows:
			return 0

		def _upsert(s: Session) -> int:
			stmt = mysql_insert(EarthquakeORM).values(rows)
			update_cols = {
				"ts_utc": stmt.inserted.ts_utc,
				"magnitude": stmt.inserted.magnitude,
				"depth_km": stmt.inserted.depth_km,
				"place": stmt.inserted.place,
				"lat": stmt.inserted.lat,
				"lon": stmt.inserted.lon,
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