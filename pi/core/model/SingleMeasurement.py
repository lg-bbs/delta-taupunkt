class SingleMeasurement:
    def __init__(self, temp: float, hum: float, dew: float | None = None):
        self.temp = temp
        self.hum = hum
        self.dew = dew

    def txt(self) -> str:
        return f"{self.temp}°C, {self.hum}%"