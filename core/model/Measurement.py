from .SingleMeasurement import SingleMeasurement

class Measurement:
    def __init__(self, time: int | None = None, inside: SingleMeasurement | None = None, outside: SingleMeasurement | None = None):
        self.time = time
        self.inside = inside
        self.outside = outside
        
    def txt() -> str:
        return f"{time}: Innen ({indide.temp}°C, {inside.hum})"