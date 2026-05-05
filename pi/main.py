from core.core import core
from config import INTERVAL
from core.API import postFatalException, postInfo

try:
    postInfo(f"Mess-System wurde gestartet")
    core(INTERVAL)
except Exception as e:
    postFatalException(e)