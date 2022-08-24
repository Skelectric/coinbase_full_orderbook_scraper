from multiprocessing import Queue
from queue import Empty
from itertools import cycle
from random import randint

from loguru import logger

from tools.timer import Timer
import signal

import numpy as np
from collections import deque, defaultdict

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtWidgets


def initialize_plotter(queue: Queue, *args, **kwargs):
    """Function to initialize and start performance plotter, required for multiprocessing."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    window = kwargs.get("window")
    performance_plotter = PerformancePlotter(queue=queue, window=window)
    logger.debug(f"Performance plotter starting.")
    performance_plotter.start()

    logger.debug("Performance plotter closed.")

    if not performance_plotter.closed:
        performance_plotter.close()


class PerformancePlotter:
    def __init__(self, queue: Queue, window: int = 1000, ):
        self.queue = queue
        self.app = pg.mkQApp("Worker Stats")
        self.pw = pg.GraphicsLayoutWidget(show=True)
        self.pw.resize(800, 300)
        self.pw.setWindowTitle('pyqtgraph: worker stats')

        # date_x_axis = pg.DateAxisItem(orientation='bottom')
        # elapsed_x_axis_1 = pg.AxisItem(orientation='bottom')

        # total count plot
        self.p1 = self.pw.addPlot(axisItems={"bottom": pg.DateAxisItem(orientation='bottom')})
        self.p1.setLabel('bottom', 'datetime.utcnow', units='seconds')
        self.p1.setLabel('left', 'total items processed')
        self.p1.setDownsampling(mode='subsample')
        self.p1.addLegend()

        # marginal count plot
        self.p2 = self.pw.addPlot(axisItems={"bottom": pg.AxisItem(orientation='bottom')})
        self.p2.setLabel('bottom', 'elapsed time', units='seconds')
        self.p2.setLabel('left', 'item processing rate')
        self.p2.setDownsampling(mode='subsample')
        # self.p2.setLogMode(x=False, y=True)

        # delay plot
        self.p3 = self.pw.addPlot(axisItems={"bottom": pg.AxisItem(orientation='bottom')})
        self.p3.setLabel('bottom', 'elapsed time', units='seconds')
        self.p3.setLabel('left', 'avg latency', units='seconds')
        self.p3.setDownsampling(mode='subsample')

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
                self.update()
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

    def update(self):

        try:
            self.update_arrays()

            self.update_p1()
            self.update_p2()
            self.update_p3()

        except KeyboardInterrupt as e:
            logger.info(f"CTRL+C KeyboardInterrupt: {e}")
            pass

    def update_p1(self):
        self.p1.clear()
        for process in self.data.keys():
            x = np.array(self.data[process]["timestamp"])
            y = np.array(self.data[process]["data"]["total"])
            if len(x) == len(y):
                item = pg.PlotCurveItem(x=x, y=y, pen=self.data[process]["pen"], name=process)
                self.p1.addItem(item)
            else:
                logger.warning(f"{process} queued unequal length arrays for plot 1:")
                logger.warning(f"timestamp = {x}")
                logger.warning(f"total = {y}")

    def update_p2(self):
        self.p2.clear()
        for process in self.data.keys():
            x = np.array(self.data[process]["elapsed"])
            y = np.array(self.data[process]["data"]["count"])
            if len(x) == len(y):
                item = pg.PlotCurveItem(x=x, y=y, pen=self.data[process]["pen"], name=process)
                self.p2.addItem(item)
            else:
                logger.warning(f"{process} queued unequal length arrays for plot 2:")
                logger.warning(f"elapsed = {x}")
                logger.warning(f"count = {y}")

    def update_p3(self):
        self.p3.clear()
        for process in self.data.keys():
            x = np.array(self.data[process]["timestamp"])
            y = np.array(self.data[process]["data"]["avg_delay"])
            if len(x) == len(y):
                item = pg.PlotCurveItem(x=x, y=y, pen=self.data[process]["pen"], name=process)
                self.p3.addItem(item)
            else:
                logger.warning(f"{process} queued unequal length arrays for plot 3:")
                logger.warning(f"elapsed = {x}")
                logger.warning(f"delay = {y}")

    def update_arrays(self):
        item = self.get_data()
        process, timestamp, elapsed, data = \
            item.get("process"), item.get("timestamp"), item.get("elapsed"), item.get("data")

        if data is None:
            msg = f"Performance plotter has received 'None' data from {process}. "
            msg += f"Removing {process} from plotting."
            logger.debug(msg)
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
                item = self.queue.get(block=False)
            except Empty:
                continue
            else:
                return item

    def remove_process(self, process: str):
        logger.debug(f"Removing {process} from stats plotting.")
        if process in self.data.keys():
            self.data.pop(process)

        # logger.debug(f"Remaining processes: {self.data.keys()}")
        if len(self.data) == 0:
            # logger.debug(f"raising NoProcessesLeft")
            raise NoProcessesLeft

    def flush_mp_queue(self):
        # logger.debug(f"Flushing queue...")
        while True:
            try:
                self.queue.get(block=False)
            except Empty:
                break


class NoProcessesLeft(Exception):
    pass
