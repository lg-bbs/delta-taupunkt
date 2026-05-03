import traceback

from pi.config import API_ENDPOINT
from pi.core.model import Measurement
import requests

def postInsertMeasurement(m: Measurement):
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

def postFatalException(exception: str, source: str = "System"):
    stack_trace = traceback.format_exc()
    postFatal(str(exception), source, details=stack_trace)

def postFatal(message: str, source: str = "System", details: str = ""):
    print(f"Fatal: {message}, {source}, {details}")
    postLog(message, 6, source, details)

def postErrorException(exception: str, source: str = "System"):
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