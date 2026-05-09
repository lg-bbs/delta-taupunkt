from core.core import core
from core.API import postFatalException, postInfo

try:
    postInfo(f"Mess-System wurde gestartet")
    core()
except Exception as e:
    postFatalException(e)