from multiprocessing import Queue
import threading
from loguru import logger
from tools.timer import Timer

import numpy as np

import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib.backend_bases import NavigationToolbar2, Event
mpl.use('TkAgg')


home = NavigationToolbar2.home


def new_home(self, *args):
    """Make pressing the home button a callable event"""
    s = 'home_event'
    event = Event(s, self)
    self.canvas.callbacks.process(s, event)
    home(self, *args)


NavigationToolbar2.home = new_home


class TimeChartPlotterMPL:
    def __init__(self, queue: Queue, title=None, ):
        self.queue = queue
        self.title = title
        # plt.ion()
        # plt.style.use('dark_background')
        # self.fig, self.ax = plt.subplots(sharex=True, sharey=True)
        self.fig = plt.figure(figsize=(9, 6))
        self.ax = self.fig.add_subplot()

        self.fig.canvas.mpl_connect('close_event', self.on_close)
        self.fig.canvas.mpl_connect('button_press_event', self.on_click)
        self.fig.canvas.mpl_connect('button_release_event', self.on_release)
        self.fig.canvas.mpl_connect('home_event', self.on_home)

        self.zoom_factory(axis=self.ax, floor_at_zero=True)

        self.timestamp = None

        self.timer = Timer()

        self.paused = False
        self.closed = False

        self.ax.xlim_prev = None
        self.ax.ylim_prev = None

        logger.debug("StatsChartPlotter initialized.")

        self.ax.set_xlabel('datetime.utcnow')
        self.ax.set_ylabel('processing time')

        threads = [thread.name for thread in threading.enumerate()]
        display_text_lower_left = (
            "Threads:",
            *threads
        )
        self.fill_misc_text(display_text_lower_left, x=0.005, y=0.25, d=-0.03)

        self.fig.canvas.draw()

    def on_close(self, event):
        # logger.debug(f"Figure closed.")
        self.closed = True
        return None

    def on_click(self, event):
        # logger.debug(f"Mouse clicked. Setting self.pause to True")
        self.paused = True

    def on_release(self, event):
        self.paused = False
        # logger.debug(f"Mouse click released. Setting self.pause to False")
        self.ax.xlim_prev = self.ax.get_xlim()
        self.ax.ylim_prev = self.ax.get_ylim()

    def on_home(self, event):
        self.ax.xlim_prev, self.ax.ylim_prev = None, None
        self.fig.canvas.draw()
        # logger.debug(f"xlim, ylim reset.")

    def plot_timer_chart(self):
        if not self.queue.empty() and not self.paused:

            timestamp, process, delta = self.get_data()
            print(timestamp, process, delta)

            self_delta = self.timer.delta()

            self.ax.scatter(timestamp, delta, color='red', label=process)

            self.ax.scatter(timestamp, self_delta, color='blue', label='plot_timer_chart')

            # final draw events ----------------------------------------------------

            self.fig.canvas.blit()

            # self.fig.canvas.flush_events()

            self.queue.task_done()

        else:
            # logger.debug(f"Plotting queue is empty or self.pause=True.")
            # time.sleep(1)
            plt.pause(0.1)
            pass

    def fill_misc_text(self, display_text, x, y, d):
        for i in range(len(display_text)):
            self.ax.text(
                x, y + i*d, display_text[i],
                horizontalalignment='left', verticalalignment='center', size='smaller',
                transform=self.ax.transAxes
            )

    def get_data(self):
        return self.queue.get()

    @staticmethod
    def zoom_factory(axis, base_scale=2e-1, floor_at_zero: bool = True):
        """returns zooming functionality to axis.
        https://gist.github.com/tacaswell/3144287"""

        def zoom_func(event, ax, scale):
            """zoom when scrolling"""
            if event.inaxes == axis:
                scale_factor = np.power(scale, -event.step)
                xlim = ax.get_xlim()
                ylim = ax.get_ylim()
                xdata = event.xdata  # get event x location
                ydata = event.ydata  # get event y location

                # Get distance from the cursor to the edge of the figure frame
                x_left = xdata - xlim[0]
                x_right = xlim[1] - xdata
                y_top = ydata - ylim[0]
                y_bottom = ylim[1] - ydata

                # set new limits
                new_xlim = [xdata - x_left * scale_factor, xdata + x_right * scale_factor]
                if floor_at_zero:
                    new_ylim = [0, ydata + y_bottom * scale_factor]
                else:
                    new_ylim = [ydata - y_top * scale_factor, ydata + y_bottom * scale_factor]

                ax.set_xlim(new_xlim)
                ax.set_ylim(new_ylim)
                ax.xlim_prev = new_xlim
                ax.ylim_prev = new_ylim
                ax.figure.canvas.draw()  # force redraw

        fig = axis.get_figure()
        fig.canvas.mpl_connect('scroll_event', lambda event: zoom_func(event, axis, 1 + base_scale))
