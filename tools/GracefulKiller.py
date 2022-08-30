import signal
from loguru import logger
import sys


class GracefulKiller:
    """Help kill threads when stop called."""
    kill_now = False

    def __init__(self, log_exit: bool = True):
        self.log_exit = log_exit
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        if sys.platform == 'win32':
            signal.signal(signal.SIGBREAK, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True
        if self.log_exit:
            logger.critical(f"Stop signal sent. kill_now = {self.kill_now}")
