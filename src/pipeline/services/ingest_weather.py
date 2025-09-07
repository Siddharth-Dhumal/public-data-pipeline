from __future__ import annotations

from dataclasses import dataclass

from pipeline.ports.extractors import WeatherExtractor
from pipeline.ports.repositories import WeatherRepository


@dataclass(frozen=True)
class IngestResult:
    fetched: int
    upserted: int


class IngestWeatherService:
    def __init__(self, extractor: WeatherExtractor, repository: WeatherRepository) -> None:
        self._extractor = extractor
        self._repository = repository

    def run(self) -> IngestResult:
        samples = list(self._extractor.fetch())
        upserted = self._repository.upsert_many(samples)
        return IngestResult(fetched=len(samples), upserted=upserted)