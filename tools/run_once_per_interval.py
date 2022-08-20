from tools.timer import Timer
from loguru import logger

def run_once(func):
    """Decorator that lets a function only run once a loop."""
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return func(*args, **kwargs)
    wrapper.has_run = False
    return wrapper


def run_once_per_interval(interval_attribute):
    """Decorator that lets a function run at most once every interval of time."""
    def decorator(method):
        def wrapper(*args, **kwargs):
            # if passed param is string, pull instance attribute
            if isinstance(interval_attribute, str):
                self = args[0]
                interval = getattr(self, interval_attribute)
                assert isinstance(interval, float)
            # otherwise, assume it's an integer and use as-is
            else:
                assert isinstance(interval_attribute, float)
                interval = interval_attribute
            # if first call, start timer and run function
            if wrapper.timer.start_time is None:
                wrapper.timer.start()
                return method(*args, **kwargs)
            # for subsequent, check timer and run function if interval of time has passed
            elif wrapper.timer.elapsed() > interval:
                wrapper.timer.reset()
                return method(*args, **kwargs)
        wrapper.timer = Timer()
        return wrapper
    return decorator


# class Foo:
#     def __init__(self):
#         self._interval = 1
#         self.counter = 0
#
#     @run_once_per_interval("_interval")
#     def hello(self):
#         logger.debug("Hello world!")
#         self.counter += 1
#
#
# if __name__ == "__main__":
#     foo = Foo()
#     while foo.counter < 5:
#         foo.hello()
