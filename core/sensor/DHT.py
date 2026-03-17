import RPi.GPIO as GPIO
import dht11

from core.model.SingleMeasurement import SingleMeasurement

class DHT:
    def __init__(self, pin: int):
        self.pin = pin

        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)

        self.sensor = dht11.DHT11(pin=self.pin)

    def measure(self) -> SingleMeasurement | None:
        print(f"first measure: {pin}")
        result = self.sensor.read()
        
        i = 0
        while not result.is_valid():
            if (i == 1000):
                return None
            print(f"INVALID measure: {pin}")
            result = self.sensor.read()
            i++
            
            
        return SingleMeasurement(
            temp = result.temperature,
            hum = result.humidity
        )