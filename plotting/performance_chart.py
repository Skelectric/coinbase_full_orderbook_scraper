from multiprocessing import Queue
from queue import Empty
from itertools import cycle

from loguru import logger

from tools.timer import Timer
import signal

import numpy as np
from collections import deque, defaultdict

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui


def initialize_plotter(queue: Queue, *args, **kwargs):
    """Function to initialize and start performance plotter, required for multiprocessing."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    window = kwargs.get("window")
    performance_plotter = PerformancePlotter(queue=queue, window=window)
    performance_plotter.start()
    logger.debug(f"Performance plotter started.")


class PerformancePlotter:
    def __init__(self, queue: Queue, window: int = 1000, ):
        self.queue = queue
        self.app = pg.mkQApp("Processing Speeds")

        axis = pg.DateAxisItem(orientation='bottom')

        self.pw = pg.PlotWidget(axisItems={"bottom": axis})
        self.pw.show()

        self.pw.addLegend()
        self.pw.setWindowTitle('pyqtgraph: Processing Speeds')
        self.pw.setLabel('bottom', 'datetime.utcnow', units='seconds')
        self.pw.setLabel('left', 'items processed')

        pen_colors = 'y', 'm', 'r', 'b', 'g', 'c', 'k', 'w'
        self.pens = cycle(pen_colors)

        # self.curve = pg.PlotCurveItem(pen='g')
        # self.pw.addItem(self.curve)

        self.fps = None
        self.qtimer = QtCore.QTimer()

        self.data = defaultdict(
            lambda: {
                "timestamp": deque(maxlen=window),
                "elapsed": deque(maxlen=window),
                "data": defaultdict(
                    lambda: deque(maxlen=window)
                ),
                "pen": None
            }
        )

        self.timer = Timer()
        self.timer.start()

    def start(self):
        self.qtimer.timeout.connect(self.display)
        self.qtimer.start()
        pg.exec()

    def close(self):
        self.app.close()

    def display(self):

        try:

            self.pw.clear()
            self.update()

            delta = self.timer.delta()
            if self.fps is None:
                self.fps = 1.0 / delta
            else:
                s = np.clip(delta * 3., 0, 1)  # todo: figure out how this works
                self.fps = self.fps * (1 - s) + (1.0 / delta) * s
            self.pw.setTitle(f"{self.fps:.2f} fps")

        # handling Ctrl+C for child processes
        except InterruptedError as e:
            logger.info(f"CTRL+C InterruptedError: {e}")
            pass
        except AttributeError as e:
            logger.info(f"CTRL+C AttributeError: {e}")
            pass
        except KeyboardInterrupt as e:
            logger.info(f"CTRL+C KeyboardInterrupt: {e}")
            pass

    def update_total(self):
        pass

    def update_count(self):
        pass

    def update_delay(self):
        pass

    def update(self):

        try:
            self.update_arrays()

            for process in self.data.keys():
                x = np.array(self.data[process]["timestamp"])
                y = np.array(self.data[process]["data"]["count"])
                # logger.debug(f"x = {x}, y = {y}")
                item = pg.PlotCurveItem(x=x, y=y, pen=self.data[process]["pen"], name=process)
                self.pw.addItem(item)

        except KeyboardInterrupt as e:
            logger.info(f"CTRL+C KeyboardInterrupt: {e}")
            pass

    def update_arrays(self):
        item = self.get_data()
        process, timestamp, elapsed, data = \
            item.get("process"), item.get("timestamp"), item.get("elapsed"), item.get("data")

        if data == "done":
            logger.debug(f"Process {process} has ended output to performance plotter queue.")
            self.remove_process(process)

        else:
            self.data[process]["timestamp"].append(timestamp)
            self.data[process]["elapsed"].append(elapsed)

            for k, v in data.items():
                self.data[process]["data"][k].append(v)

            if self.data[process]["pen"] is None:  # set line color only once per process
                self.data[process]["pen"] = next(self.pens)

    def get_data(self):
        while True:
            try:
                item = self.queue.get()
            except Empty:
                continue
            else:
                return item

    def remove_process(self, process: str):
        if process in self.data.keys():
            self.data.pop(process)
