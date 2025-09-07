from __future__ import annotations

from dataclasses import dataclass

from pipeline.ports.extractors import EarthquakeExtractor
from pipeline.ports.repositories import EarthquakeRepository


@dataclass(frozen=True)
class IngestResult:
    fetched: int
    upserted: int


class IngestEarthquakesService:
    def __init__(self, extractor: EarthquakeExtractor, repository: EarthquakeRepository) -> None:
        self._extractor = extractor
        self._repository = repository

    def run(self) -> IngestResult:
        events = list(self._extractor.fetch())
        upserted = self._repository.upsert_many(events)
        return IngestResult(fetched=len(events), upserted=upserted)