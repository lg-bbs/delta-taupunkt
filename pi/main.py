from core.core import core
from pi.config import INTERVAL
from pi.core.API import postFatalException, postInfo

try:
    postInfo(f"Mess-System wurde gestartet")
    core(INTERVAL)
except Exception as e:
    postFatalException(e)