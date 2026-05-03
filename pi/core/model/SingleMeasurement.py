class SingleMeasurement:
    def __init__(self, temp: float, hum: float, dew: float):
        self.temp = temp
        self.hum = hum
        self.dew = dew

    def txt(self) -> str:
        return f"{self.temp}°C, {self.hum}% >> {self.dew}°C"