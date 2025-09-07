from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable
from pipeline.domain.models import WeatherSample, EarthquakeEvent

class WeatherExtractor(ABC):
    @abstractmethod
    def fetch(self) -> Iterable[WeatherSample]:
        raise NotImplementedError

class EarthquakeExtractor(ABC):
    @abstractmethod
    def fetch(self) -> Iterable[EarthquakeEvent]:
        raise NotImplementedError