# core.py
import time
from .sensor.DHTHelper import DHTHelper
from .db.db import MeasureDB
from .sensor.LCD import LCD

def core(interval: int = 2):
    dhtHelper = DHTHelper()
    lcd = LCD()
    db = MeasureDB()
    
    print("start")
    while True:
        print("loop")
        m = dhtHelper.measure_all()
        print("after measure")

        if m:
            print(m.txt())
            lcd.showData(m)
            print("after show")
            db.insert_measurement(m)
            print("after save")
        else:
            print("Messung ungültig")

        time.sleep(interval)