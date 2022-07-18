import json
import logging
import time

def save_json(filename, d):
    """Save d into json file."""
    with open(filename, 'w') as j:
        json_string = json.dumps(d)
        j.write(json_string)
        logging.debug(f"Saved {d.__name__} to {filename}.")

def load_json(filename):
    """Load from json"""
    with open(filename, 'r') as j:
        logging.debug(f"Opened {filename}...")
        return json.load(j)


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""


class Timer:
    def __init__(self):
        self._start_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def reset(self):
        """Reset timer."""
        self._start_time = time.perf_counter()

    def check(self):
        """Check elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        return elapsed_time

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        print(f"Elapsed time: {elapsed_time:0.4f} seconds")