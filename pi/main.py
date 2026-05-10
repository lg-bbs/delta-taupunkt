from core.core import core
from core.API import checkApiAvailability, postFatalException, postInfo

try:
    checkApiAvailability()
    postInfo(f"Mess-System wurde gestartet")
    core()
except Exception as e:
    postFatalException(e)