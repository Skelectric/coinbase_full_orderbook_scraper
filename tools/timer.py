import time


class Timer:
    def __init__(self) -> None:
        self.start_time = None
        self.last_time = None

    def start(self) -> None:
        """Start a new timer"""
        if self.start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self.start_time = time.perf_counter()

    def reset(self) -> None:
        """Reset timer."""
        self.start_time = time.perf_counter()

    def lap(self):
        elapsed = self.elapsed()
        self.reset()
        return elapsed

    def elapsed(self, hms_format: bool = False, display: bool = False) -> int | str:
        """Print elapsed time, return elapsed time in seconds as an int"""
        if self.start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self.start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)

        if display:
            print(f"Elapsed time: {int(hours):0>2}:{int(minutes):0>2}:{int(seconds):05.2f}")

        if hms_format:
            return self.seconds_to_hms(elapsed_time)

        return elapsed_time

    def delta(self):
        if self.start_time is None:
            self.start()

        if self.last_time is None:
            self.last_time = self.start_time

        now = time.perf_counter()
        delta = now - self.last_time
        self.last_time = now

        return delta

    @staticmethod
    def seconds_to_hms(seconds: int) -> str:
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{int(hours):0>2}:{int(minutes):0>2}:{int(seconds):05.2f}"


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""
    pass
