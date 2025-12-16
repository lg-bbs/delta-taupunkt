from SingleMeasurement import SingleMeasurement

class Measurement:
    def __init__(self, time: int, inside: SingleMeasurement, outside: SingleMeasurement):
        self.time = time
        self.inside = inside
        self.outside = outside