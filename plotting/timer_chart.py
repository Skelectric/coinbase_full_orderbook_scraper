from multiprocessing import Queue
from queue import Empty

from loguru import logger
from tools.timer import Timer

import numpy as np
from collections import deque, defaultdict

# import matplotlib as mpl
# from matplotlib import pyplot as plt
# import matplotlib.animation as anim
# from matplotlib.backend_bases import NavigationToolbar2, Event
# mpl.use('TkAgg')

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui


class PerformancePlotter:
    def __init__(self, queue: Queue, title=None, ):
        self.queue = queue
        self.app = pg.mkQApp("Processing Times")

        axis = pg.DateAxisItem(orientation='bottom')
        self.pw = pg.PlotWidget(axisItems={"bottom": axis})
        self.pw.show()

        self.pw.addLegend()
        self.pw.setWindowTitle('pyqtgraph: Processing Times')
        self.pw.setLabel('bottom', 'datetime.utcnow', units='seconds')
        self.pw.setLabel('left', 'items per sec')

        # self.curve = pg.PlotCurveItem(pen='g')
        # self.pw.addItem(self.curve)

        self.fps = None
        self.qtimer = QtCore.QTimer()
        self.data = defaultdict(lambda: [deque(maxlen=300), deque(maxlen=300)])

        self.timer = Timer()

    def start(self):
        self.qtimer.timeout.connect(self.display)
        self.qtimer.start()
        pg.exec()

    def close(self):
        self.pw.close()

    def display(self):

        self.pw.clear()
        self.update()

        delta = self.timer.delta()
        if self.fps is None:
            self.fps = 1.0 / delta
        else:
            s = np.clip(delta * 3., 0, 1)  # todo: figure out what this is doing
            self.fps = self.fps * (1 - s) + (1.0 / delta) * s
        self.pw.setTitle(f"{self.fps:.2f} fps")

    def update(self):
        self.update_arrays()

        for process in self.data.keys():
            x = np.array(self.data[process][0])
            y = np.array(self.data[process][1])
            x_bounds = x[0] - 0.5, x[-1] + 0.5
            y_bounds = 0, max(y) * 1.1
            # logger.debug(f"x = {x}, y = {y}")
            item = pg.PlotCurveItem(x=x, y=y, pen='b', name=process)
            # self.curve.setData(x=x, y=y)
            self.pw.setRange(xRange=x_bounds, yRange=y_bounds)
            self.pw.addItem(item)

    def update_arrays(self):
        timestamp, process, delta = self.get_data()

        self.data[process][0].append(timestamp)
        self.data[process][1].append(delta)

    def get_data(self):
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

# home = NavigationToolbar2.home
#
#
# def new_home(self, *args):
#     """Make pressing the home button a callable event"""
#     s = 'home_event'
#     event = Event(s, self)
#     self.canvas.callbacks.process(s, event)
#     home(self, *args)
#
#
# NavigationToolbar2.home = new_home
#
#
# class TimeChartPlotterMPL:
#     def __init__(self, queue: Queue, title=None, ):
#         self.queue = queue
#         self.title = title
#         # plt.ion()
#         # plt.style.use('dark_background')
#         # self.fig, self.ax = plt.subplots(sharex=True, sharey=True)
#         self.fig = plt.figure(figsize=(9, 6))
#         self.ax = self.fig.add_subplot()
#
#         self.fig.canvas.mpl_connect('close_event', self.on_close)
#         self.fig.canvas.mpl_connect('button_press_event', self.on_click)
#         self.fig.canvas.mpl_connect('button_release_event', self.on_release)
#         self.fig.canvas.mpl_connect('home_event', self.on_home)
#
#         self.zoom_factory(axis=self.ax, floor_at_zero=True)
#
#         self.timestamp = None
#
#         self.timer = Timer()
#
#         self.paused = False
#         self.closed = False
#
#         self.ax.xlim_prev = None
#         self.ax.ylim_prev = None
#
#         logger.debug("StatsChartPlotter initialized.")
#
#         self.ax.set_xlabel('datetime.utcnow')
#         self.ax.set_ylabel('processing time')
#
#         threads = [thread.name for thread in threading.enumerate()]
#         display_text_lower_left = (
#             "Threads:",
#             *threads
#         )
#         self.fill_misc_text(display_text_lower_left, x=0.005, y=0.25, d=-0.03)
#
#         self.fig.canvas.draw()
#
#     def on_close(self, event):
#         # logger.debug(f"Figure closed.")
#         self.closed = True
#         return None
#
#     def on_click(self, event):
#         # logger.debug(f"Mouse clicked. Setting self.pause to True")
#         self.paused = True
#
#     def on_release(self, event):
#         self.paused = False
#         # logger.debug(f"Mouse click released. Setting self.pause to False")
#         self.ax.xlim_prev = self.ax.get_xlim()
#         self.ax.ylim_prev = self.ax.get_ylim()
#
#     def on_home(self, event):
#         self.ax.xlim_prev, self.ax.ylim_prev = None, None
#         self.fig.canvas.draw()
#         # logger.debug(f"xlim, ylim reset.")
#
#     def plot_timer_chart(self):
#         if not self.queue.empty() and not self.paused:
#
#             timestamp, process, delta = self.get_data()
#             print(timestamp, process, delta)
#
#             self_delta = self.timer.delta()
#
#             self.ax.scatter(timestamp, delta, color='red', label=process)
#
#             self.ax.scatter(timestamp, self_delta, color='blue', label='plot_timer_chart')
#
#             # final draw events ----------------------------------------------------
#
#             self.fig.canvas.blit()
#
#             # self.fig.canvas.flush_events()
#
#             self.queue.task_done()
#
#         else:
#             # logger.debug(f"Plotting queue is empty or self.pause=True.")
#             # time.sleep(1)
#             plt.pause(0.1)
#             pass
#
#     def fill_misc_text(self, display_text, x, y, d):
#         for i in range(len(display_text)):
#             self.ax.text(
#                 x, y + i*d, display_text[i],
#                 horizontalalignment='left', verticalalignment='center', size='smaller',
#                 transform=self.ax.transAxes
#             )
#
#     def get_data(self):
#         return self.queue.get()
#
#     @staticmethod
#     def zoom_factory(axis, base_scale=2e-1, floor_at_zero: bool = True):
#         """returns zooming functionality to axis.
#         https://gist.github.com/tacaswell/3144287"""
#
#         def zoom_func(event, ax, scale):
#             """zoom when scrolling"""
#             if event.inaxes == axis:
#                 scale_factor = np.power(scale, -event.step)
#                 xlim = ax.get_xlim()
#                 ylim = ax.get_ylim()
#                 xdata = event.xdata  # get event x location
#                 ydata = event.ydata  # get event y location
#
#                 # Get distance from the cursor to the edge of the figure frame
#                 x_left = xdata - xlim[0]
#                 x_right = xlim[1] - xdata
#                 y_top = ydata - ylim[0]
#                 y_bottom = ylim[1] - ydata
#
#                 # set new limits
#                 new_xlim = [xdata - x_left * scale_factor, xdata + x_right * scale_factor]
#                 if floor_at_zero:
#                     new_ylim = [0, ydata + y_bottom * scale_factor]
#                 else:
#                     new_ylim = [ydata - y_top * scale_factor, ydata + y_bottom * scale_factor]
#
#                 ax.set_xlim(new_xlim)
#                 ax.set_ylim(new_ylim)
#                 ax.xlim_prev = new_xlim
#                 ax.ylim_prev = new_ylim
#                 ax.figure.canvas.draw()  # force redraw
#
#         fig = axis.get_figure()
#         fig.canvas.mpl_connect('scroll_event', lambda event: zoom_func(event, axis, 1 + base_scale))
