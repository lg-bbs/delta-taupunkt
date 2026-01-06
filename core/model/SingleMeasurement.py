class SingleMeasurement:
    def __init__(self, temp: float, hum: float):
        self.temp = temp
        self.hum = hum

    def txt(self) -> str:
        return f"{self.temp}°C, {self.hum}"