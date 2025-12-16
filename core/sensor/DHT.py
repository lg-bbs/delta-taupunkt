import RPi.GPIO as GPIO
import dht11

from model.Measurement import Measurement

class DHT:
    def __init__(self, pin: int):
        self.pin = pin

        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)

        self.sensor = dht11.DHT11(pin=self.pin)

    def measure(self) -> Measurement | None:
        result = self.sensor.read()

        if result.is_valid():
            return Measurement(
                temperature = result.temperature,
                humidity = result.humidity
            )

        return None