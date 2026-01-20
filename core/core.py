# core.py
import time
from .sensor.DHTHelper import DHTHelper

def core(interval: int = 2):
    dhtHelper = DHTHelper()
    
    while True:
        m = dhtHelper.measure_all()

        if m:
            print(m.txt())
        else:
            print("Messung ungültig")

        time.sleep(interval)