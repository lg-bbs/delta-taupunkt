import time
from .DHT import DHT
from core.model.SingleMeasurement import SingleMeasurement
from core.model.Measurement import Measurement

class DHTHelper:
    def __init__(self):
            self.insideDHT = DHT(pin=4)
    
    def measure_all() -> Measurement | None:
        return Measurement(
            time.time_ns() // 1_000_000,
            inside = insideDHT.measure()
        )