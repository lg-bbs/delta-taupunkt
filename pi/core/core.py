import time

from pi.core.API import postErrorException, postInfo, postInsertMeasurement, postWarn
from pi.core.calc.BasicCalc import addDewToMeasurement, calcFanRunning
from pi.core.sensor import Fan
from .sensor.DHTHelper import DHTHelper
from .db.db import MeasureDB
from .sensor.LCD import LCD

def core(interval: int = 2):
    dhtHelper = DHTHelper()
    lcd = LCD()
    db = MeasureDB()
    fan = Fan()
    fanRunning = False

    postInfo(f"Hauptprozess wurde mit einem Intervall von {interval} gestartet", "Core")
    while True:
        try:
            print("loop")
            m = dhtHelper.measure_all()
            print("after measure")        

            if m:
                print(m.txt())

                try:
                    addDewToMeasurement(m)
                    print("after adding dew")

                    fanRunning = calcFanRunning(m, fanBefore=fanRunning)
                    print("after fan calc")

                    if fanRunning:
                        fan.turn_on()
                    else:
                        fan.turn_off()
                    print("after fan set")

                    lcd.showData(m)
                    print("after show")

                    db.insert_measurement(m)
                    print("after save")

                    postInsertMeasurement(m)
                    print("after post")
                except Exception as e:
                    postErrorException(e, "Core-Measurement-Control")
            else:
                postWarn("Messung ungültig")

            time.sleep(interval)
        except Exception as e:
            postErrorException(e, "Core-Loop")