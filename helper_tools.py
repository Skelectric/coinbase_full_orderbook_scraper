import json
from loguru import logger
import time
from urllib import parse
from threading import Lock

def save_json(filename, d):
    """Save d into json file."""
    with open(filename, 'w') as j:
        json_string = json.dumps(d)
        j.write(json_string)
        logger.add(f"Saved {d.__name__} to {filename}.")

def load_json(filename):
    """Load from json"""
    with open(filename, 'r') as j:
        logger.add(f"Opened {filename}...")
        return json.load(j)


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""
    # Todo: set this up


class Timer:
    def __init__(self) -> None:
        self.start_time = None

    def start(self) -> None:
        """Start a new timer"""
        if self.start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self.start_time = time.time()

    def reset(self) -> None:
        """Reset timer."""
        self.start_time = time.time()

    def elapsed(self, hms_format: bool = False, display: bool = False) -> int | str:
        """Print elapsed time, return elapsed time in seconds as an int"""
        if self.start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.time() - self.start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)

        if display:
            print(f"Elapsed time: {int(hours):0>2}:{int(minutes):0>2}:{int(seconds):05.2f}")

        if hms_format:
            return self.seconds_to_hms(elapsed_time)

        return elapsed_time

    @staticmethod
    def seconds_to_hms(seconds: int) -> str:
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{int(hours):0>2}:{int(minutes):0>2}:{int(seconds):05.2f}"

def url_fix(s, charset='UTF-8'):
    scheme, netloc, path, qs, anchor = parse.urlsplit(s)
    path = parse.quote(path, '/%')
    qs = parse.quote_plus(qs, ':&=')
    return parse.urlunsplit((scheme, netloc, path, qs, anchor))

def s_print(s_print_lock: Lock, *a, **b):
    """Thread-safe print function."""
    with s_print_lock:
        print(*a, **b)
