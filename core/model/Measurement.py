from .SingleMeasurement import SingleMeasurement

class Measurement:
    def __init__(self, time: int | None = None, inside: SingleMeasurement | None = None, outside: SingleMeasurement | None = None):
        self.time = time
        self.inside = inside
        self.outside = outside
        
    def txt(self) -> str:
        if not self.isValid():
            return f"{self.time}: Keine Daten"
        return f"{self.time}: Innen ({self.inside.txt()})"
        
    def isValid(self) -> bool:
        return self.inside is not None