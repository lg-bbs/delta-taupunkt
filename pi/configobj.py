from dataclasses import dataclass

from config import HUM_INSIDE_OFFSET, HUM_OUTSIDE_OFFSET, HYSTERESE, INTERVAL, MIN_TEMP_INSIDE, MIN_TEMP_OUTSIDE, TARGET_DELTA, TEMP_INSIDE_OFFSET, TEMP_OUTSIDE_OFFSET

@dataclass
class Config:
    id: int = 0
    interval: int = INTERVAL
    targetDelta: float = TARGET_DELTA
    hysrerese: float = HYSTERESE
    minTempInside: float = MIN_TEMP_INSIDE
    minTempOutside: float = MIN_TEMP_OUTSIDE
    tempInsideOffset: float = TEMP_INSIDE_OFFSET
    humInsideOffset: float = HUM_INSIDE_OFFSET
    tempOutsideOffset: float = TEMP_OUTSIDE_OFFSET
    humOutsideOffset: float = HUM_OUTSIDE_OFFSET

    @classmethod
    def from_dict(cls, data: dict):
        """Erstellt eine Config-Instanz aus einem Dictionary (API Response)."""
        return cls(**data)