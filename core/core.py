# core.py
import time
from .sensor.DHTHelper import measure_all

def core(interval: int = 2):
    while True:
        m = measure_all()

        if m:
            print(m.txt())
        else:
            print("Messung ungültig")

        time.sleep(interval)