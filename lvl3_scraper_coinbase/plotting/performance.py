from queue import Empty
from itertools import cycle
from random import randint
from datetime import datetime, timedelta
import pprint
from threading import Lock
from loguru import logger
import copy

from tools.timer import Timer
from tools.configure_loguru import configure_logger
from tools.run_once_per_interval import run_once_per_interval

import signal

import numpy as np
from scipy.ndimage import uniform_filter1d

from collections import deque, defaultdict

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore

import sys
if sys.platform == 'darwin':
    from tools.mp_queue_OSX import Queue
else:
    from multiprocessing import Queue


def initialize_plotter(queue: Queue, *args, **kwargs):
    """Function to initialize and start performance plotter, required for multiprocessing."""

    # configure logging
    output_directory = kwargs.get("output_directory", None)
    module_timer = kwargs.get("module_timer", Timer())
    if module_timer.get_start_time() is None:
        module_timer.start()
    module_timestamp = module_timer.get_start_time(_format="datetime_utc").strftime("%Y%m%d-%H%M%S")
    log_filename = f"performance_plotter_log_{module_timestamp}.log"
    log_to_file = kwargs.get("log_to_file", False)
    configure_logger(log_to_file, output_directory, log_filename)

    # ignore keyboard interrupts
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    window = kwargs.get("window")
    perf_plot_interval = kwargs.get("perf_plot_interval", 0.1)  # how often other processes will send data

    performance_plotter = PerformancePlotter(queue=queue, window=window, perf_plot_interval=perf_plot_interval)
    logger.debug(f"Performance plotter starting.")
    performance_plotter.start()

    logger.debug("Performance plotter closed.")

    if not performance_plotter.closed:
        performance_plotter.close()


class TimeAxisItem(pg.AxisItem):
    def __init__(self, *args, **kwargs):
        super(TimeAxisItem, self).__init__(*args, **kwargs)

    def tickStrings(self, values, scale, spacing):
        return [str(timedelta(seconds=value)) for value in values]


class PerformancePlotter:
    """Expects queue items in this format:
            item = {
                "process": "[process]",
                "timestamp": datetime.utcnow().timestamp(),
                "elapsed": self.module_timer.elapsed(),
                "data": {
                    "total": self.total_count,
                    "count": self.count,
                    "avg_delay": np.mean(self.delays) if len(self.delays) != 0 else 0,
                    ...
                },
            }
    """
    def __init__(self, queue: Queue, window: int = 1000, *args, **kwargs):
        self.queue = queue
        self.app = pg.mkQApp("Worker Stats")
        self.pw = pg.GraphicsLayoutWidget(show=True)
        self.pw.resize(900, 600)
        self.pw.setWindowTitle('pyqtgraph: worker stats')

        # date_x_axis = pg.DateAxisItem(orientation='bottom')
        # elapsed_x_axis_1 = pg.AxisItem(orientation='bottom')

        # total count plot
        self.p1 = self.pw.addPlot(axisItems={"bottom": pg.DateAxisItem(orientation='bottom')})
        self.p1.setLabel('bottom', 'datetime.utcnow')
        self.p1.setLabel('left', 'total items processed')
        self.p1.setDownsampling(mode='subsample')
        self.p1.addLegend()

        # marginal count plot
        self.p2 = self.pw.addPlot(axisItems={"bottom": TimeAxisItem(orientation='bottom')})
        self.p2.setLabel('bottom', 'elapsed time')
        self.p2.setLabel('left', 'item processing rate')
        self.p2.setDownsampling(mode='subsample')

        self.pw.nextRow()

        # latency plot
        self.p3 = self.pw.addPlot(axisItems={"bottom": TimeAxisItem(orientation='bottom')})
        self.p3.setLabel('bottom', 'elapsed time')
        self.p3.setLabel('left', 'avg latency', units='seconds')
        self.p3.setDownsampling(mode='subsample')

        # time delta plot
        self.p4 = self.pw.addPlot(axisItems={"bottom": TimeAxisItem(orientation='bottom')})
        self.p4.setLabel('bottom', 'elapsed time')
        self.p4.setLabel('left', 'timedelta', units='seconds')
        self.p4.setDownsampling(mode='subsample')
        self.p4.addLegend()

        # queue size plot
        self.p5 = self.pw.addPlot(axisItems={"bottom": TimeAxisItem(orientation='bottom')})
        self.p5.setLabel('bottom', 'elapsed time')
        self.p5.setLabel('left', 'queue size', units='items')
        self.p5.setDownsampling(mode='subsample')
        self.p5.addLegend()

        # self.pw.show()

        pen_colors = 'y', 'm', 'c', 'r', 'b', 'g', 'w'
        self.pens = cycle(pen_colors)

        # choose random color to start with
        idx = randint(0, len(pen_colors))
        for i in range(idx):
            next(self.pens)

        self.fps = None
        self.qtimer = QtCore.QTimer()

        self.data = defaultdict(
            lambda: {
                "timestamp": np.full(window, np.nan),
                "elapsed": np.full(window, np.nan),
                "data": defaultdict(
                    lambda: np.full(window, np.nan),
                ),
                "pen": None
            }
        )

        # performance stats for self
        self.timer = Timer()
        self.timer.start()
        self.perf_plot_interval = kwargs.get("perf_plot_interval", 0.1)  # can update self stats this much

        self.closed = False

    def start(self):
        self.qtimer.timeout.connect(self.display)
        self.qtimer.start()
        pg.exec()

    def close(self):
        self.flush_mp_queue()
        self.queue.close()  # required for ending QueueFeederThread and raising ValueError to exit event loop
        self.app.closeAllWindows()
        self.closed = True

    def display(self):

        try:
            try:
                self.update_data()
                self.update_p1()
                self.update_p2()
                self.update_p3()
                self.update_p4()
                self.update_self_stats()
            # haven't figured out a way to stop event loop without raising ValueError
            except ValueError:
                logger.debug(f"Performance plotter exited loop via Exception")
                pass
            else:
                delta = self.timer.delta()
                if self.fps is None:
                    self.fps = 1.0 / delta
                else:
                    s = np.clip(delta * 3., 0, 1)  # todo: figure out how this works
                    self.fps = self.fps * (1 - s) + (1.0 / delta) * s
                self.p2.setTitle(f"{self.fps:.2f} fps")

        except NoProcessesLeft:
            self.close()

    @run_once_per_interval("perf_plot_interval")
    def update_self_stats(self):
        self.update_data_for_self()
        self.update_p5()

    def update_p1(self):
        self.p1.clear()
        for process in (key for key in self.data.keys() if key != "performance_plotter"):
            item = self.make_curve_item(process, "timestamp", "total")
            if item is not None:
                self.p1.addItem(item)

    def update_p2(self):
        self.p2.clear()
        for process in (key for key in self.data.keys() if key != "performance_plotter"):
            item = self.make_curve_item(process, "elapsed", "marginal", (True, 20))
            if item is not None:
                self.p2.addItem(item)

    def update_p3(self):
        self.p3.clear()
        for process in (key for key in self.data.keys() if key != "performance_plotter"):
            item = self.make_curve_item(process, "elapsed", "avg_latency")
            if item is not None:
                self.p3.addItem(item)

    def update_p4(self):
        self.p4.clear()
        for process in (key for key in self.data.keys()):
            item = self.make_curve_item(process, "elapsed", "delta", (True, 100))
            if item is not None:
                self.p4.addItem(item)

    def update_p5(self):
        self.p5.clear()
        process = "performance_plotter"
        item = self.make_curve_item(process, "elapsed", "queue_size", (True, 10))
        if item is not None:
            self.p5.addItem(item)

    def make_curve_item(
            self, process: str, x_var: str, y_var: str,
            moving_avg: (bool, int) = (False, 10), preserve_ymax: bool = False
    ) -> pg.PlotCurveItem | None:

        x = self.data[process].get(x_var, None)
        y = self.data[process]["data"].get(y_var, None)

        if x is None or y is None:
            return None

        x = x[~np.isnan(x)]
        y = y[~np.isnan(y)]

        moving_avg_flag, moving_avg_window = moving_avg
        if moving_avg_flag:
            y = uniform_filter1d(y, moving_avg_window)

        if len(x) == len(y):
            item = pg.PlotCurveItem(x=x, y=y, pen=self.data[process]["pen"], name=process)
            return item
        else:
            logger.warning(f"{process} queued unequal length arrays!")
            logger.warning(f"{x_var} = {x}")
            logger.warning(f"{y_var} = {y}")

    def update_data(self):
        item = self.get_data()
        process, timestamp, elapsed, data = \
            item.get("process"), item.get("timestamp"), item.get("elapsed"), item.get("data")

        self.latest_timestamp = timestamp

        if data is None:
            msg = f"Performance plotter has received 'None' data from {process}. "
            msg += f"Removing {process} from plotting."
            logger.debug(msg)
            self.remove_process(process)

        else:
            self.np_append(self.data[process]["timestamp"], timestamp)
            self.np_append(self.data[process]["elapsed"], elapsed)

            for k, v in data.items():
                self.np_append(self.data[process]["data"][k], v)

            self.choose_pen(process)

    def update_data_for_self(self):
        """track elapsed and queue size for self"""
        self.np_append(self.data["performance_plotter"]["timestamp"], datetime.utcnow().timestamp())
        self.np_append(self.data["performance_plotter"]["elapsed"], self.timer.elapsed())
        qsize = self.queue.qsize()
        self.np_append(self.data["performance_plotter"]["data"]["queue_size"], qsize)
        self.choose_pen("performance_plotter")

    def choose_pen(self, process: str) -> None:
        """Sets a line color only once per process."""
        if self.data[process]["pen"] is None:
            # noinspection PyTypedDict
            self.data[process]["pen"] = next(self.pens)

    def get_data(self) -> any:
        while True:
            try:
                item = self.queue.get(block=False)
            except Empty:
                continue
            else:
                return item

    def remove_process(self, process: str) -> None:
        logger.debug(f"Removing {process} from stats plotting.")
        if process in self.data.keys():
            self.data.pop(process)

        # logger.debug(f"Remaining processes: {self.data.keys()}")
        if list(self.data.keys()) == ['performance_plotter']:
            raise NoProcessesLeft

    def flush_mp_queue(self):
        # logger.debug(f"Flushing queue...")
        while True:
            try:
                self.queue.get(block=False)
            except Empty:
                break

    @staticmethod
    def np_append(np_array, item) -> None:
        """Append to a numpy array and rotate as if it's a deque."""
        np_array[:-1] = np_array[1:]  # shift all elements left 1 step
        np_array[-1] = item  # place item into last element

    def print_data_readable(self):
        data_as_dict = {k: v for k, v in self.data.items()}
        for process in data_as_dict:
            for key in data_as_dict[process]:
                if isinstance(data_as_dict[process][key], deque):
                    data_as_dict[process][key] = list(data_as_dict[process][key])
                if isinstance(data_as_dict[process][key], defaultdict):
                    data_as_dict[process][key] = dict(data_as_dict[process][key])
                    for data in data_as_dict[process][key]:
                        if isinstance(data_as_dict[process][key][data], deque):
                            data_as_dict[process][key][data] = list(data_as_dict[process][key][data])

        pp = pprint.PrettyPrinter(indent=4)

        print_lock = Lock()
        with print_lock:
            pp.pprint(data_as_dict)


class NoProcessesLeft(Exception):
    pass


class PerfPlotQueueItem(dict):
    """Dictionary object with the specific format that the performance chart plotter expects.
    Has methods for counting, tracking latency, and tracking processing times (via _delta method)"""
    def __init__(
            self,
            process: str,
            count: bool = True,
            latency: bool = True,
            delta: bool = False,
            *args, **kwargs
    ):
        super(PerfPlotQueueItem, self).__init__(*args, **kwargs)

        self["process"] = process
        self["elapsed"] = None
        self["timestamp"] = None
        self["data"] = {}

        if count:
            self.total = 0  # running total counter
            self.marginal = 0  # counter that gets reset, useful for tracking item processing rate
            self["data"].update({"total": None, "marginal": None})

        if latency:
            self["data"].update({"avg_latency": None})
            self.latencies = deque()

        if delta:
            self["data"].update({"delta": None})
            self.deltas = deque()

        self.timer = kwargs.get("module_timer", Timer())
        if self.timer.get_start_time() is None:
            self.timer.start()

    def timedelta(self, log: bool = False) -> float:
        """Returns
        Use log=True to include the delta in the final item calc"""
        delta = self.timer.delta()
        if log:
            self.deltas.append(delta)
        self._update()
        return delta

    def latency(self, timestamp: float):
        """Pass this method a timestamp to log the difference between utcnow and the timestamp."""
        self.latencies.append(max(datetime.utcnow().timestamp() - timestamp, 0))

    def count(self):
        """Increment counters"""
        self.total += 1
        self.marginal += 1

    def track(self, **kwargs) -> dict:
        """Increment counters, log delta, and log latency in one call"""
        log_time = kwargs.get("log_time", True)

        if hasattr(self, "marginal"):
            self.count()

        if hasattr(self, "latency"):
            timestamp = kwargs.get("timestamp", None)
            if timestamp is not None:
                self.latency(timestamp)

        if hasattr(self, "deltas"):
            self.timedelta(log=log_time)

        return self

    def _update(self) -> dict:
        """Return a dict item ready for placing into performance plotter queue"""

        self["timestamp"] = datetime.utcnow().timestamp()
        self["elapsed"] = self.timer.elapsed()

        if self["data"] is not None:
            if hasattr(self, "marginal"):
                self["data"]["total"] = self.total
                self["data"]["marginal"] = self.marginal

            if hasattr(self, "latencies"):
                self["data"]["avg_latency"] = np.mean(self.latencies) if len(self.latencies) != 0 else 0

            if hasattr(self, "deltas"):
                self["data"]["delta"] = np.mean(self.deltas) if len(self.deltas) != 0 else 0

        return self

    def signal_end_item(self) -> dict:
        """Return a dict item that will signal to performance plotter that this is the last item"""

        self["timestamp"] = datetime.utcnow().timestamp()
        self["elapsed"] = self.timer.elapsed()
        self["data"] = None

        return self

    def send_to_queue(self, queue: Queue, log: bool = False):
        """Send self's dict object into queue and clear iterables/counters"""
        self._update()
        item = copy.deepcopy(self)
        queue.put(item)
        if log:
            logger.debug(f"Placed into queue: {item}")
        self.reset()

    def clear(self):
        """Override dict's clear method"""
        self.reset()

    def reset(self) -> dict:
        """Clear iterables/counters except running total. Also reset timedelta"""

        if hasattr(self, "latencies"):
            self.latencies.clear()
        if hasattr(self, "marginal"):
            self.marginal = 0
        if hasattr(self, "deltas"):
            self.deltas.clear()
            self.timedelta()

        self._update()
        return self


if __name__ == "__main__":
    # testing
    import time
    test_queue = Queue()
    a = PerfPlotQueueItem("dummy")
    print(a)
    a.track()
    print(a)
    time.sleep(1)
    a.track()
    print(a)
    time.sleep(0.5)
    a.track()
    print(a)
    a.send_to_queue(test_queue)
    print("sent to queue")
    print(a)
    time.sleep(0.5)
    print("pulling from queue")
    a.signal_end_item()
    print(a)



