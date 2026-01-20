# core.py
import time
from .sensor.DHTHelper import DHTHelper
from .sensor.LCD import LCD

def core(interval: int = 2):
    dhtHelper = DHTHelper()
    lcd = LCD()
    
    while True:
        m = dhtHelper.measure_all()

        if m:
            print(m.txt())
            lcd.showData(m)
        else:
            print("Messung ungültig")

        time.sleep(interval)