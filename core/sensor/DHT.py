import RPi.GPIO as GPIO
import dht11

from model.SingleMeasurement import SingleMeasurement

class DHT:
    def __init__(self, pin: int):
        self.pin = pin

        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)

        self.sensor = dht11.DHT11(pin=self.pin)

    def measure(self) -> SingleMeasurement | None:
        result = self.sensor.read()

        if result.is_valid():
            return SingleMeasurement(
                temp = result.temperature,
                hum = result.humidity
            )

        return None