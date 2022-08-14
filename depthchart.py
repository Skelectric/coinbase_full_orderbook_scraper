import queue

import numpy as np
from loguru import logger
from matplotlib import pyplot as plt


class DepthChartPlotter:
    # Todo: Build multi-orderbook support
    def __init__(self, title=None, max_queue_size=1):
        self.title = title
        self.queue = queue.Queue(maxsize=max_queue_size)
        plt.ion()
        plt.style.use('dark_background')
        # self.fig, self.ax = plt.subplots(sharex=True, sharey=True)
        self.fig = plt.figure(figsize=(9, 6))
        self.ax = self.fig.add_subplot()

        self.display_lim = 0.01  # display % below best bid and % above best ask
        self.outlier_pct = 0.01  # remove outliers % below best bid and % above best ask
        self.move_price_pct = 0.005  # will calculate how much $ to move price by this amount

        self.timestamp = None
        self.sequence = None
        self.unique_traders = None

        # self.skip_item_freq = 50

        logger.debug("DepthChartPlotter initialized.")

    def plot(self):
        # logger.debug(f"Plotting data #{self.item_num}")
        if not self.queue.empty():
            bid_prices, bid_depth, bid_liquidity, ask_prices, ask_depth, ask_liquidity = self.get_data()

            if self.ax.lines:  # clear previously drawn lines
                self.ax.cla()

            self.ax.set_xlabel('Price')
            self.ax.set_ylabel('Quantity')

            best_bid, best_ask, worst_bid, worst_ask = None, None, None, None
            max_bid_depth, max_ask_depth = None, None
            x_min, x_max, y_max = None, None, None

            if bid_prices is not None:
                best_bid = bid_prices[-1]
                worst_bid = bid_prices[0]
                x_min = max(best_bid * (1 - self.display_lim), worst_bid)
                self.ax.step(bid_prices, bid_depth, color="green", label="bids")
                plt.fill_between(bid_prices, bid_depth, facecolor="green", step='pre', alpha=0.2)
                max_bid_depth = max(bid_depth)

            if ask_prices is not None:
                best_ask = ask_prices[0]
                worst_ask = ask_prices[-1]
                x_max = min(best_ask * (1 + self.display_lim), worst_ask)
                self.ax.step(ask_prices, ask_depth, color="red", label="asks")
                plt.fill_between(ask_prices, ask_depth, facecolor="red", step='pre', alpha=0.2)
                max_ask_depth = max(ask_depth)

            self.ax.set_xlim(left=x_min, right=x_max)
            self.ax.spines['bottom'].set_position('zero')

            # operations that require both bids and asks not to be None
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
                try:
                    ask_liq = ask_liquidity[ask_liq_index]
                except IndexError:
                    ask_liq = ask_liquidity[ask_liq_index-1]

                price_moved_down = mid * (1 - self.move_price_pct)
                bid_liq_index = np.searchsorted(bid_prices, price_moved_down, side='right')
                try:
                    bid_liq = bid_liquidity[bid_liq_index]
                except IndexError:
                    bid_liq = bid_liquidity[bid_liq_index-1]

                bid_ask_txt = f"bid/ask/spread/mid {best_bid:,.3f} / {best_ask:,.3f}"
                mid_spread_txt = f"mid/spread {mid:.3f} / {bid_ask_spread:.3f}"
                worst_bid_ask_txt = f"worst bid {worst_bid:.3f}... worst ask {worst_ask:.3f}."
                ask_liquidity_txt = f"Amount to move price up {self.move_price_pct:.2%} "
                ask_liquidity_txt += f"to {price_moved_up:,.2f}: ${ask_liq:,.2f}"
                bid_liquidity_txt = f"Amount to move price down {self.move_price_pct:.2%} "
                bid_liquidity_txt += f"to {price_moved_down:,.2f}: ${bid_liq:,.2f}"

                y_max = max(max_bid_depth, max_ask_depth) * 1.1

                self.ax.legend(loc='upper right')  # placed here to prevent "No artist" error msg from printing

            self.ax.set_ylim(top=y_max)

            self.fig.suptitle(f"Market Depth - {self.title}")
            self.ax.set_title(f"latest timestamp: {self.timestamp}")

            display_text = (
                f"discarding bottom/top {self.outlier_pct:.1%} of bid/ask levels.",
                f"displaying orderbook within {self.display_lim:.1%} of best bid/ask.",
                f"worst bid/ask after discard: {worst_bid_ask_txt}",
                f"{self.unique_traders} unique client ids",
                bid_ask_txt,
                mid_spread_txt,
                ask_liquidity_txt,
                bid_liquidity_txt
            )

            self.fill_misc_text(display_text, x=0.005, y=0.98, d=-0.03)

            self.fig.canvas.flush_events()  # Todo: read more about what this does
            self.fig.canvas.draw()

            self.queue.task_done()

        else:
            # logger.debug(f"Plotting queue is empty. Sleeping for 2 second.")
            # plt.pause(0.1)
            pass

    def fill_misc_text(self, display_text, x, y, d):
        for i in range(len(display_text)):
            self.ax.text(
                x, y + i*d, display_text[i],
                horizontalalignment='left', verticalalignment='center', size='smaller',
                transform=self.ax.transAxes
            )

    def get_data(self):

        # skip processing items when qsize too large
        # while self.queue.qsize() > self.skip_item_freq:
        #     _ = self.queue.get()

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
