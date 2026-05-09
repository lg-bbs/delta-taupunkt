import time

from core.API import getConfig, postErrorException, postInfo, postInsertMeasurement, postWarn
from core.calc.BasicCalc import addDewToMeasurement, calcFanRunning
#from core.sensor.Fan import Fan
from configobj import Config
from core.model.Measurement import Measurement
from core.model.SingleMeasurement import SingleMeasurement
#from .sensor.DHTHelper import DHTHelper
from .db.db import MeasureDB
#from .sensor.LCD import LCD

def core():
    #dhtHelper = DHTHelper()
    #lcd = LCD()
    db = MeasureDB()
    #fan = Fan()
    fanRunning = False
    config = Config()

    try:
        config = getConfig()
    except Exception as e:
        postErrorException(e, "Config")

    interval = config.interval

    print(f"Nutze config: {config}")

    postInfo(f"Hauptprozess wurde mit einem Intervall von {interval} gestartet", "Core")
    while True:
        try:
            print("loop")

            try:
                config = getConfig()
                interval = config.interval
            except Exception as e:
                postErrorException(e, "Config")

            #m = dhtHelper.measure_all()
            inside = SingleMeasurement(temp=20.0, hum=50.0)
            outside = SingleMeasurement(temp=15.0, hum=60.0)
            m = Measurement(
                time.time_ns() // 1_000_000,
                inside = inside,
                outside = outside
            )
            print("after measure")        

            if m:
                print(m.txt())

                m.correctByConfig(config)
                print("after correct")
                print(m.txt())

                try:
                    addDewToMeasurement(m)
                    print("after adding dew")

                    fanRunning = calcFanRunning(m, fanBefore=fanRunning, config=config)
                    print(f"Fan running: {fanRunning}")
                    print("after fan calc")

                #    if fanRunning:
                #        fan.turn_on()
                #    else:
                #        fan.turn_off()
                    print("after fan set")

                #    lcd.showData(m)
                    print("after show")

                    db.insert_measurement(m)
                    print("after save")

                    postInsertMeasurement(m)
                    print("after post")
                except Exception as e:
                    postErrorException(e, "Core-Measurement-Control")
            else:
                postWarn("Messung ungültig")

            
        except Exception as e:
            postErrorException(e, "Core-Loop")

        time.sleep(interval)