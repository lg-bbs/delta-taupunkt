import time
from .DHT import DHT
from core.model.SingleMeasurement import SingleMeasurement
from core.model.Measurement import Measurement

class DHTHelper:
    def __init__(self):
            self.insideDHT = DHT(pin=4)
            self.outsideDHT = DHT(pin=37)
    
    def measure_all(self) -> Measurement | None:
        return Measurement(
            time.time_ns() // 1_000_000,
            inside = self.insideDHT.measure(),
            outside = self.outsideDHT.measure()
        )