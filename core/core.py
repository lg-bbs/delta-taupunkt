# core.py
import time
from .sensor.DHTHelper import DHTHelper
from .db.MeasureDB import MeasureDB
from .sensor.LCD import LCD

def core(interval: int = 2):
    dhtHelper = DHTHelper()
    lcd = LCD()
    db = MeasureDB()
    
    while True:
        m = dhtHelper.measure_all()

        if m:
            print(m.txt())
            lcd.showData(m)
            db.insert_measurement(m)
        else:
            print("Messung ungültig")

        time.sleep(interval)