import signal
from loguru import logger
import sys

class GracefulKiller:
    """Help kill threads when stop called."""
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        if sys.platform == 'win32':
            signal.signal(signal.SIGBREAK, self.exit_gracefully)

    def exit_gracefully(self, *args):
        logger.critical("Stop signal sent. Stopping threads.............................")
        self.kill_now = True