import json
import logging

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