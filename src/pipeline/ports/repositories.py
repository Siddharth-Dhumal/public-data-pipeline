from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from pipeline.domain.models import WeatherSample, EarthquakeEvent


class WeatherRepository(ABC):
    @abstractmethod
    def upsert_many(self, samples: Iterable[WeatherSample]) -> int:
        raise NotImplementedError

class EarthquakeRepository(ABC):

    @abstractmethod
    def upsert_many(self, events: Iterable[EarthquakeEvent]) -> int:
        raise NotImplementedError