import time
from .DHT import DHT
from core.model.SingleMeasurement import SingleMeasurement
from core.model.Measurement import Measurement

def measure_all() -> Measurement | None:
    insideDHT = DHT(pin=4)

    return Measurement(
        time.time_ns() // 1_000_000,
        inside = insideDHT.measure()
    )