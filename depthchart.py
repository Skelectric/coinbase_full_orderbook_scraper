from queue import Queue, Empty

import numpy as np
from loguru import logger
from tools.helper_tools import Timer
import threading
import easygui

import matplotlib
from matplotlib import pyplot as plt
from matplotlib.backend_bases import NavigationToolbar2, Event
matplotlib.use('TkAgg')

home = NavigationToolbar2.home


def new_home(self, *args):
    """Make pressing the home button a callable event"""
    s = 'home_event'
    event = Event(s, self)
    self.canvas.callbacks.process(s, event)
    home(self, *args)


NavigationToolbar2.home = new_home


class DepthChartPlotter:
    # Todo: Build multi-orderbook support
    def __init__(self, queue: Queue, title=None, ):
        self.queue = queue
        self.title = title
        plt.ion()
        plt.style.use('dark_background')
        # self.fig, self.ax = plt.subplots(sharex=True, sharey=True)
        self.fig = plt.figure(figsize=(9, 6))
        self.ax = self.fig.add_subplot()

        self.fig.canvas.mpl_connect('close_event', self.on_close)
        self.fig.canvas.mpl_connect('button_press_event', self.on_click)
        self.fig.canvas.mpl_connect('button_release_event', self.on_release)
        self.fig.canvas.mpl_connect('home_event', self.on_home)

        self.zoom_factory(axis=self.ax, depth_chart=True)

        self.display_pct_default = (0.01, 0.01)
        self.display_pct = list(self.display_pct_default)  # display % below best bid and % above best ask
        self.outlier_pct = 0.01  # remove outliers % below best bid and % above best ask
        self.move_price_pct = 0.005  # will calculate how much $ to move price by this amount

        self.timestamp = None
        self.sequence = None
        self.unique_traders = None

        self.__timer = Timer()
        self.__timer.start()

        self.paused = False
        self.closed = False

        self.ax.xlim_prev = None
        self.ax.ylim_prev = None

        logger.debug("DepthChartPlotter initialized.")

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
        self.display_pct = list(self.display_pct_default)
        self.fig.canvas.draw()
        # logger.debug(f"xlim, ylim reset.")

    def plot_depth_chart(self):
        if not self.queue.empty() and not self.paused:

            if self.ax.lines:  # clear previously drawn lines
                self.ax.cla()

            # get data and plot step functions
            bid_prices, bid_depth, bid_liquidity, ask_prices, ask_depth, ask_liquidity = self.get_data()

            best_bid, best_ask, worst_bid, worst_ask = None, None, None, None

            if bid_prices is not None:
                best_bid = bid_prices[-1]
                worst_bid = bid_prices[0]
                self.ax.step(bid_prices, bid_depth, color="green", label="bids")
                plt.fill_between(bid_prices, bid_depth, facecolor="green", step='pre', alpha=0.2)

            if ask_prices is not None:
                best_ask = ask_prices[0]
                worst_ask = ask_prices[-1]
                self.ax.step(ask_prices, ask_depth, color="red", label="asks")
                plt.fill_between(ask_prices, ask_depth, facecolor="red", step='pre', alpha=0.2)

            # operations that require both bids and asks -------------------------------
            # default text
            if best_bid is None or best_ask is None:
                bid_ask_txt = f"bid/ask LOADING..."
                mid_spread_txt = f"mid/spread LOADING..."
                worst_bid_ask_txt = f"lowest bid LOADING... highest ask LOADING..."
                ask_liquidity_txt = f"Ask liquidity LOADING..."
                bid_liquidity_txt = f"Bid liquidity LOADING..."

            else:

                bid_ask_spread = best_ask - best_bid
                mid = (best_ask + best_bid) / 2

                # calculate liquidity
                price_moved_up = mid * (1 + self.move_price_pct)
                ask_liq_index = np.searchsorted(ask_prices, price_moved_up, side='left')
                ask_liq = ask_liquidity[ask_liq_index-1]

                price_moved_down = mid * (1 - self.move_price_pct)
                bid_liq_index = np.searchsorted(bid_prices, price_moved_down, side='right')
                bid_liq = bid_liquidity[bid_liq_index-1]

                bid_ask_txt = f"bid/ask/spread/mid {best_bid:,.3f} / {best_ask:,.3f}"
                mid_spread_txt = f"mid/spread {mid:.3f} / {bid_ask_spread:.3f}"
                worst_bid_ask_txt = f"worst bid {worst_bid:.3f}... worst ask {worst_ask:.3f}."
                ask_liquidity_txt = f"Amount to move price up {self.move_price_pct:.2%} "
                ask_liquidity_txt += f"to {price_moved_up:,.2f}: ${ask_liq:,.2f}"
                bid_liquidity_txt = f"Amount to move price down {self.move_price_pct:.2%} "
                bid_liquidity_txt += f"to {price_moved_down:,.2f}: ${bid_liq:,.2f}"

                self.ax.legend(loc='upper right')  # placed here to prevent "No artist" error msg from printing

            # calc xlim and ylim ------------------------------------------------------------

            max_bid_depth_displayed, max_ask_depth_displayed = None, None
            x_min, x_max, y_min, y_max = None, None, 0, None

            # calc display boundaries
            if best_bid is not None:
                if self.ax.xlim_prev is None:
                    x_min = max(best_bid * (1 - self.display_pct[0]), worst_bid)
                else:
                    x_min = self.ax.xlim_prev[0]
                    self.display_pct[0] = (best_bid - x_min) / best_bid
                x_min_index = np.searchsorted(bid_prices, x_min, side='right')
                max_bid_depth_displayed = bid_depth[x_min_index - 1]

            if best_ask is not None:
                if self.ax.xlim_prev is None:
                    x_max = min(best_ask * (1 + self.display_pct[1]), worst_ask)
                else:
                    x_max = self.ax.xlim_prev[1]
                    self.display_pct[1] = (x_max - best_ask) / x_max
                x_max_index = np.searchsorted(ask_prices, x_max, side='left')
                max_ask_depth_displayed = ask_depth[x_max_index - 1]

            if best_bid is not None and best_ask is not None:
                if self.ax.ylim_prev is None:
                    y_max = max(max_bid_depth_displayed, max_ask_depth_displayed) * 1.1
                else:
                    y_max = self.ax.ylim_prev[1]

            # finalize xlim and ylim for current draw
            self.ax.set_xlim(left=x_min, right=x_max)
            self.ax.set_ylim(bottom=y_min, top=y_max)

            # final text and formatting  -----------------------------------------------------------

            self.ax.spines['bottom'].set_position('zero')

            self.ax.set_xlabel('Price')
            self.ax.set_ylabel('Quantity')

            self.fig.suptitle(f"Market Depth - {self.title}")
            self.ax.set_title(f"latest timestamp: {self.timestamp}")

            display_text_upper_left = (
                f"Displaying prices {self.display_pct[0]:.1%} below best bid and {self.display_pct[1]:.1%} above best ask.",
                f"discarding bottom/top {self.outlier_pct:.1%} of bid/ask levels.",
                f"worst bid/ask after discard: {worst_bid_ask_txt}",
                bid_ask_txt,
                mid_spread_txt,
                ask_liquidity_txt,
                bid_liquidity_txt,
                f"draw-time = {self.__timer.lap():.4f} sec"
            )

            threads = [thread.name for thread in threading.enumerate()]
            display_text_lower_left = (
                "Threads:",
                *threads
            )

            self.fill_misc_text(display_text_upper_left, x=0.005, y=0.98, d=-0.03)
            self.fill_misc_text(display_text_lower_left, x=0.005, y=0.25, d=-0.03)

            # final draw events ----------------------------------------------------

            self.fig.canvas.draw()

            self.fig.canvas.flush_events()

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

        data = self.queue.get()
        self.timestamp, self.sequence, self.unique_traders, \
            bid_levels, ask_levels = \
            data.get("timestamp"), data.get("sequence"), data.get("unique_traders"), \
            data.get("bid_levels"), data.get("ask_levels")

        return self.transform_data(bid_levels, ask_levels, self.outlier_pct)

    @staticmethod
    def transform_data(bid_levels, ask_levels, outlier_pct) -> tuple:
        bid_prices, bid_depth, ask_prices, ask_depth = None, None, None, None
        bid_liquidity, ask_liquidity = None, None

        if bid_levels != {}:
            # get bid_levels and ask_levels, place into numpy arrays, and sort
            bids = np.fromiter(bid_levels.items(), dtype="f,f")
            bids.sort()
            # remove bottom % of bids
            remove_bids = len(bids) - int(len(bids) * (1 - outlier_pct))
            bids = bids[remove_bids:]
            if len(bids) != 0:
                # split into prices and sizes (reverse order for bids only)
                bid_prices = np.vectorize(lambda x: x[0])(bids[::-1])
                bid_sizes = np.vectorize(lambda x: x[1])(bids[::-1])
                # calculate order depth
                bid_depth = np.cumsum(bid_sizes)
                # for bids only, zip, sort and split to reverse order again
                bid_depth_zip = np.fromiter(zip(bid_prices, bid_depth), dtype='f,f')
                bid_depth_zip.sort()
                bid_prices = np.vectorize(lambda x: x[0])(bid_depth_zip)
                bid_depth = np.vectorize(lambda x: x[1])(bid_depth_zip)
                bid_liquidity = np.multiply(bid_prices, bid_depth)

        if ask_levels != {}:  # repeat for asks
            asks = np.fromiter(ask_levels.items(), dtype="f,f")
            asks.sort()
            remove_asks = int(len(asks) * (1 - outlier_pct))
            asks = asks[:remove_asks]
            if len(asks) != 0:
                ask_prices = np.vectorize(lambda x: x[0])(asks)
                ask_sizes = np.vectorize(lambda x: x[1])(asks)
                ask_depth = np.cumsum(ask_sizes)
                ask_liquidity = np.multiply(ask_prices, ask_depth)

        return bid_prices, bid_depth, bid_liquidity, ask_prices, ask_depth, ask_liquidity

    def zoom_factory(self, axis, base_scale=2e-1, depth_chart: bool = True):
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
                if depth_chart:
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
