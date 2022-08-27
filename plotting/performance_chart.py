from multiprocessing import Queue
from queue import Empty
from itertools import cycle
from random import randint
from datetime import datetime, timedelta
import pprint
from threading import Lock

from loguru import logger
import sys

from tools.timer import Timer
import signal

import numpy as np
from scipy.ndimage import uniform_filter1d

from collections import deque, defaultdict

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtWidgets

def configure_logger():
    logger.remove()  # remove default logger
    # # add file logger with full debug
    # logger.add(
    #     "logs\\performance_plotter_log_{time}.log", level="DEBUG", rotation="10 MB"
    # )

    # add console logger with formatting
    logger_format = "<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> "
    logger_format += "--- <level>{level}</level> | Thread {thread} <level>{message}</level>"
    logger.add(
        sys.stdout, level="DEBUG",
        format=logger_format,
    )


def initialize_plotter(queue: Queue, *args, **kwargs):
    """Function to initialize and start performance plotter, required for multiprocessing."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    configure_logger()
    window = kwargs.get("window")
    performance_plotter = PerformancePlotter(queue=queue, window=window)
    logger.debug(f"Performance plotter starting.")
    performance_plotter.start()

    logger.debug("Performance plotter closed.")

    if not performance_plotter.closed:
        performance_plotter.close()


class TimeAxisItem(pg.AxisItem):
    def __init__(self, *args, **kwargs):
        super(TimeAxisItem, self).__init__(*args, **kwargs)
        # # Paint tick every 5 seconds
        # self.setTickSpacing(5, 0, 0)
        # # Set fixed tick height
        # self.fixedHeight = 150

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
    def __init__(self, queue: Queue, window: int = 1000, module_timer=None):
        self.queue = queue
        self.app = pg.mkQApp("Worker Stats")
        self.pw = pg.GraphicsLayoutWidget(show=True)
        self.pw.resize(600, 600)
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

        # delay plot
        self.p3 = self.pw.addPlot(axisItems={"bottom": TimeAxisItem(orientation='bottom')})
        self.p3.setLabel('bottom', 'elapsed time')
        self.p3.setLabel('left', 'avg latency', units='seconds')
        self.p3.setDownsampling(mode='subsample')

        # performance plotter queue size
        self.p4 = self.pw.addPlot(axisItems={"bottom": TimeAxisItem(orientation='bottom')})
        self.p4.setLabel('bottom', 'elapsed time')
        self.p4.setLabel('left', 'queue size', units='items')
        self.p4.setDownsampling(mode='subsample')
        self.p4.addLegend()

        # self.pw.show()

        pen_colors = 'y', 'm', 'c', 'r', 'b', 'g', 'w'
        self.pens = cycle(pen_colors)

        # choose random color to start with
        idx = randint(0, len(pen_colors))
        for i in range(idx):
            next(self.pens)

        self.fps = None
        self.qtimer = QtCore.QTimer()

        # self.data = defaultdict(
        #     lambda: {
        #         "timestamp": deque(maxlen=window),
        #         "elapsed": deque(maxlen=window),
        #         "data": defaultdict(
        #             lambda: deque(maxlen=window)
        #         ),
        #         "pen": None
        #     }
        # )

        self.data = defaultdict(
            lambda: {
                "timestamp": np.negative(np.ones(window)),
                "elapsed": np.negative(np.ones(window)),
                "data": defaultdict(
                    lambda: np.negative(np.ones(window))
                ),
                "pen": None
            }
        )

        self.counter = 0

        # performance stats for self
        self.timer = Timer()
        self.timer.start()

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
                self.update_data_for_self()
                self.update_p1()
                self.update_p2()
                self.update_p3()
                self.update_p4()
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

    def update_p1(self):
        self.p1.clear()
        for process in (key for key in self.data.keys() if key != "performance_plotter"):
            item = self.make_curve_item(process, "timestamp", "total")
            self.p1.addItem(item)

    def update_p2(self):
        self.p2.clear()
        for process in (key for key in self.data.keys() if key != "performance_plotter"):
            item = self.make_curve_item(process, "elapsed", "count", (True, 20))
            self.p2.addItem(item)

    def update_p3(self):
        self.p3.clear()
        for process in (key for key in self.data.keys() if key != "performance_plotter"):
            item = self.make_curve_item(process, "elapsed", "avg_delay")
            self.p3.addItem(item)

    def update_p4(self):
        self.p4.clear()
        process = "performance_plotter"
        item = self.make_curve_item(process, "elapsed", "queue_size", (True, 10))
        self.p4.addItem(item)

    def make_curve_item(
            self, process: str, x_var: str, y_var: str,
            moving_avg: (bool, int) = (False, 10), preserve_ymax: bool = False
    ) -> pg.PlotCurveItem:
        assert x_var in {"timestamp", "elapsed"}, "x_var is not 'timestamp' or 'elapsed'!"
        msg = f"y_var is not an available datapoint! choices are: {self.data[process]['data'].keys()}"
        assert y_var in self.data[process]["data"].keys(), msg

        x = self.data[process][x_var]
        y = self.data[process]["data"][y_var]

        x = x[x != -1]
        y = y[y != -1]

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
            # self.data[process]["timestamp"].append(timestamp)
            # self.data[process]["elapsed"].append(elapsed)
            self.np_append(self.data[process]["timestamp"], timestamp)
            self.np_append(self.data[process]["elapsed"], elapsed)

            for k, v in data.items():
                self.np_append(self.data[process]["data"][k], v)

            self.choose_pen(process)

    def update_data_for_self(self):
        # track elapsed and queue size for self
        self.np_append(self.data["performance_plotter"]["timestamp"], datetime.utcnow().timestamp())
        self.np_append(self.data["performance_plotter"]["elapsed"], self.timer.elapsed())
        qsize = self.queue.qsize()
        self.np_append(self.data["performance_plotter"]["data"]["queue_size"], qsize)

        # try:
        #     qsize_prev = self.data["performance_plotter"]["data"]["queue_size"][-1]
        # except IndexError:
        #     qsive_prev = qsize
        #
        # self.np_append(self.data["performance_plotter"]["data"]["queue_size_delta"], qsize_prev)

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
