import time

from core.API import getConfig, postErrorException, postInfo, postInsertMeasurement, postWarn, checkApiAvailability
from core.calc.BasicCalc import addDewToMeasurement, calcFanRunning
from configobj import Config
from core.model.Measurement import Measurement
from core.model.SingleMeasurement import SingleMeasurement
from config import TEST_MODE
from .db.db import MeasureDB

if TEST_MODE:
    print("TEST MODE IST AKTIVIERT - Es werden keine Messungen gepostet und der Lüfter wird nicht angesteuert, sondern nur die Logik durchlaufen")
else:
    from core.sensor.Fan import Fan
    from .sensor.DHTHelper import DHTHelper
    from .sensor.LCD import LCD

def core():
    db = MeasureDB()
    fanRunning = False
    config = Config()
    if not TEST_MODE:
        dhtHelper = DHTHelper()
        lcd = LCD()
        fan = Fan()

    try:
        remoteConfig = getConfig()
        if remoteConfig:
            config = remoteConfig
    except Exception as e:
        postErrorException(e, "Config")

    interval = config.interval

    print(f"Nutze config: {config}")

    postInfo(f"Hauptprozess wurde mit einem Intervall von {interval} gestartet", "Core")
    while True:
        try:
            print("loop")

            try:
                remoteConfig = getConfig()
                if remoteConfig:
                    config = remoteConfig
                    interval = config.interval
            except Exception as e:
                postErrorException(e, "Config")

            if not TEST_MODE:
                m = dhtHelper.measure_all()
            else:
                inside = SingleMeasurement(temp=20.0, hum=50.0)
                outside = SingleMeasurement(temp=15.0, hum=60.0)
                m = Measurement(
                    time.time_ns() // 1_000_000,
                    inside = inside,
                    outside = outside
                )

            print("after measure")        

            if m:
                
                if not m.inside:
                    postWarn("Messung innen ungültig", "InDHT", "Timeout bei der Messung des Innensensors.")
                    continue
                
                if not m.outside:
                    postWarn("Messung außen ungültig", "OutDHT", "Timeout bei der Messung des Außensensors.")
                    continue
                
                m.correctByConfig(config)
                print("after correct")
                print(m.txt())

                try:
                    addDewToMeasurement(m)
                    print("after adding dew")

                    fanRunning = calcFanRunning(m, fanBefore=fanRunning, config=config)

                    print("")
                    print("++++++++++++++++++++++++++++++++")
                    print(f"Fan running: {fanRunning}")
                    print(m.txt())
                    print("++++++++++++++++++++++++++++++++")
                    print("")

                    if not TEST_MODE:
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

                    print("")
                    print("")
                    print("------------------------------")
                    print("")
                except Exception as e:
                    postErrorException(e, "Core-Measurement-Control")
            else:
                postWarn("Messung ungültig", "DHT")
                
            try:
                checkApiAvailability()
            except Exception as e:
                print("API Status konnte nicht ermittelt werden")

            
        except Exception as e:
            postErrorException(e, "Core-Loop")

        time.sleep(interval)