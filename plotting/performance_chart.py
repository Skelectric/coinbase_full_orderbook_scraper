import time
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
    logger.debug(f"Performance plotter starting...")
    performance_plotter.start()


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

        self.fps = None
        self.qtimer = QtCore.QTimer()

        # timestamp, counter, pen color
        # deque implementation
        # self.data = defaultdict(lambda: [deque(maxlen=window), deque(maxlen=window), None])
        # numpy implementation
        self.data = defaultdict(
            lambda: [
                np.zeros(window, dtype=np.float),
                np.zeros(window, dtype=np.int),
                None
            ]
        )

        self.timer = Timer()

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

    def update(self) -> None:

        try:
            self.update_arrays()

            # self.timer.reset()
            for process in self.data.keys():

                # deque implementation
                # x = np.array(self.data[process][0])
                # y = np.array(self.data[process][1])

                # numpy implementation
                x = self.data[process][0]
                x = x[x != 0]
                y = self.data[process][1]
                y = y[y != 0]

                pen = self.data[process][2]
                item = pg.PlotCurveItem(x=x, y=y, pen=pen, name=process)
                self.pw.addItem(item)

            # print(self.timer.lap())

        except KeyboardInterrupt as e:
            logger.info(f"CTRL+C KeyboardInterrupt: {e}")
            pass

    def update_arrays(self) -> None:
        timestamp, process, counter = self.get_data()

        try:
            assert isinstance(timestamp, float), f"timestamp '{timestamp}' is type {type(timestamp)}"
            assert isinstance(process, str), f"process '{process}' is type {type(process)}"
            assert isinstance(counter, int) or counter == 'done', f"counter '{counter}' is type {type(counter)}"
        except AssertionError as e:
            logger.debug(f"AssertionError: {e}")
        else:
            if counter == "done":
                logger.debug(f"Process {process} has ended output to performance plotter queue.")
                self.remove_process(process)
            else:
                # deque implementation
                # self.data[process][0].append(timestamp)
                # self.data[process][1].append(counter)

                # numpy implementation
                self.np_append(self.data[process][0], timestamp)
                self.np_append(self.data[process][1], counter)

                if self.data[process][2] is None:  # set line color only once per process
                    self.data[process][2] = next(self.pens)

    def get_data(self) -> tuple:
        # logger.debug(f"getting data")
        while True:
            try:
                item = self.queue.get()
            except Empty:
                # logger.debug(f"empty")
                continue
            else:
                # logger.debug(f"Got item {item}")
                return item

    def remove_process(self, process: str) -> None:
        if process in self.data.keys():
            self.data.pop(process)

    @staticmethod
    def np_append(np_array, item) -> None:
        """Append to a numpy array and rotate as if it's a deque."""
        np_array[:-1] = np_array[1:]  # shift all elements left 1 step
        np_array[-1] = item  # place item into last element
