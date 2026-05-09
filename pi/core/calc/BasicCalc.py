import math

from core.model import Measurement
from configobj import Config

def calcFanRunning(m: Measurement, fanBefore: bool, config: Config) -> bool:
    deltaTP = m.inside.dew - m.outside.dew
    run = fanBefore
    if (deltaTP > config.targetDelta + config.hysrerese):
        run = True
    
    if (deltaTP < config.targetDelta):
        run = False
    
    if (m.inside.temp < config.minTempInside or m.outside.temp < config.minTempOutside):
        run = False
    
    m.isFanRunning = run
    return run

def addDewToMeasurement(m: Measurement):
    m.inside.dew = taupunkt(m.inside.temp, m.inside.hum)
    m.outside.dew = taupunkt(m.outside.temp, m.outside.hum)

def taupunkt(t, r):
    # Parameter a und b basierend auf der Temperatur wählen
    if t >= 0:
        a = 7.5
        b = 237.3
    else:
        a = 7.6
        b = 240.7
    
    # Sättigungsdampfdruck in hPa
    sdd = 6.1078 * (10**((a * t) / (b + t)))
    
    # Dampfdruck in hPa
    dd = sdd * (r / 100)
    
    # v-Parameter
    v = math.log10(dd / 6.1078)
    
    # Taupunkttemperatur (°C)
    tt = (b * v) / (a - v)
    
    return tt