import json
from loguru import logger
import time
from urllib import parse
from threading import Lock
import string

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

    def lap(self):
        elapsed = self.elapsed()
        self.reset()
        return elapsed

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

def s_print(*a, **b):
    """Thread-safe print function."""
    s_print_lock = Lock()
    with s_print_lock:
        print(*a, **b)

def class_user_interface(class_instance):
    """Prints list of class instance's callable methods, prompts for input, and calls the chosen method"""
    while True:

        # list methods in class
        print(f"\n{class_instance.__class__.__name__} methods:\n")

        method_list = [
            attribute for attribute in dir(class_instance)
            if callable(getattr(class_instance, attribute))  # excludes properties and attributes
               and attribute.startswith('__') is False  # excludes special methods
               and attribute != "user_interface"  # excludes self (valid if this is included as a method)
        ]
        addl_actions = ["list methods again", "exit"]
        method_list.extend(addl_actions)

        for i, method in enumerate(method_list):
            print(i, method)
            if i == len(method_list)-len(addl_actions)-1:
                print("------------")

        # prompt user for method, and accept keyword params
        user_action, *params = input(f"\nEnter: ").split(',')

        # convert params from string into dict
        params = dict(param.strip(" ").split("=") for param in params)

        print(params)

        while True:
            try:
                user_action = int(user_action)
            except Exception:
                user_action = input(f"Enter valid selection: ")
                continue
            else:
                if user_action not in range(len(method_list)):
                    user_action = input(f"Enter valid selection: ")
                    continue
                else:
                    break

        # get method from string and call the method
        if method_list[user_action] == "list methods again":
            continue
        elif method_list[user_action] == "exit":
            break
        else:
            method = getattr(class_instance, method_list[user_action])
            method(params)
