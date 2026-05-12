import traceback

from config import API_ENDPOINT
from core.model import Measurement
import requests
from configobj import Config

API_AVAILABLE = True

def checkApiAvailability():
    global API_AVAILABLE
    try:
        # Kurzer Check (Timeout von 1 Sekunde reicht lokal völlig aus)
        requests.head(API_ENDPOINT, timeout=1)
        API_AVAILABLE = True
        print("API ist erreichbar")
    except requests.exceptions.RequestException:
        API_AVAILABLE = False
        print("API ist nicht erreichbar")

def postInsertMeasurement(m: Measurement):
    if not API_AVAILABLE:
        print("API ist nicht verfügbar, überspringe das Posten der Messung")
        return
    try:
        url = f"{API_ENDPOINT}measurements"
        params = {
            "insideTemp": m.inside.temp,
            "insideHum": m.inside.hum,
            "insideDew": m.inside.dew,
            "outsideTemp": m.outside.temp,
            "outsideHum": m.outside.hum,
            "outsideDew": m.outside.dew,
            "isFanRunning": str(m.isFanRunning).lower()
        }

        response = requests.post(url, params=params)
        print(response.status_code)
    except Exception as e:
        postErrorException(e, "API")

def postFatalException(exception: Exception, source: str = "System"):
    stack_trace = traceback.format_exc()
    postFatal(str(exception), source, details=stack_trace)

def postFatal(message: str, source: str = "System", details: str = ""):
    print(f"Fatal: {message}, {source}, {details}")
    postLog(message, 6, source, details)

def postErrorException(exception: Exception, source: str = "System"):
    stack_trace = traceback.format_exc()
    postError(str(exception), source, details=stack_trace)

def postError(message: str, source: str = "System", details: str = ""):
    print(f"Error: {message}, {source}, {details}")
    postLog(message, 5, source, details)

def postWarn(message: str, source: str = "System", details: str = ""):
    print(f"Warn: {message}, {source}, {details}")
    postLog(message, 4, source, details)

def postInfo(message: str, source: str = "System", details: str = ""):
    print(f"Info: {message}, {source}, {details}")
    postLog(message, 3, source, details)

def postSuccess(message: str, source: str = "System", details: str = ""):
    print(f"Success: {message}, {source}, {details}")
    postLog(message, 2, source, details)

def postDebug(message: str, source: str = "System", details: str = ""):
    print(f"Debug: {message}, {source}, {details}")
    postLog(message, 1, source, details)

def postLog(message: str, severity: int, source: str = "System", details: str = ""):
    if not API_AVAILABLE:
        print("API ist nicht verfügbar, überspringe das Posten des Logs")
        return
    try:
        url = f"{API_ENDPOINT}logs"
        params = {
            "message": message,
            "severity": str(severity),
            "source": source,
            "details": details
        }

        response = requests.post(url, params=params)
        print(response.status_code)
    except Exception as e:
        print(f"Ein unerwarteter Fehler beim Post-Log ist aufgetreten: {e}")

def getConfig():
    if not API_AVAILABLE:
        print("API ist nicht verfügbar, überspringe das Get der Config")
        return None
    try:
        url = f"{API_ENDPOINT}config"
        response = requests.get(url)
        response.raise_for_status()
        configJson = response.json()
        postDebug(f"Online-Config wurde geladen: {response.text}")
        return Config.from_dict(configJson)
    except Exception as e:
        postErrorException(e, "Config")
        return None