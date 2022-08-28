import json
import threading

from loguru import logger
from urllib import parse
from threading import Lock

from lvl3_scraper_coinbase.tools.run_once_per_interval import run_once_per_interval


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


def log_active_threads():
    interval = 20  # seconds

    @run_once_per_interval(interval)
    def _log_active_threads():
        logger.debug(f"Active threads:")
        for thread in threading.enumerate():
            logger.debug(thread.name)
        logger.debug(f"...")

    _log_active_threads()
