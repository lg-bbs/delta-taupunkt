from configobj import Config

from .SingleMeasurement import SingleMeasurement

class Measurement:
    def __init__(self, time: int | None = None, inside: SingleMeasurement | None = None, outside: SingleMeasurement | None = None, isFanRunning: bool | None = None):
        self.time = time
        self.inside = inside
        self.outside = outside
        self.isFanRunning = isFanRunning
        
    def txt(self) -> str:
        if not self.isValid():
            return f"{self.time}: Keine Daten"
        return f"I: {self.inside.txt()}\nA: {self.outside.txt()}"
        
    def isValid(self) -> bool:
        return self.inside is not None
    
    def correctByConfig(self, config: Config):
        if self.inside is not None:
            self.inside.temp += config.tempInsideOffset
            self.inside.hum += config.humInsideOffset
        if self.outside is not None:
            self.outside.temp += config.tempOutsideOffset
            self.outside.hum += config.humOutsideOffset