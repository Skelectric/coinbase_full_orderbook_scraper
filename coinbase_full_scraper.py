"""Build orderbook using webhook to Coinbase's FULL channel."""
# Todo: Build multi-market support

from loguru import logger


logger.remove()  # remove default logger

from datetime import datetime
from queue import Queue
import sys
import itertools
from pathlib import Path
import time
import easygui

import pandas as pd
pd.options.display.float_format = '{:.6f}'.format

# homebrew modules
from api_coinbase import CoinbaseAPI
from api_coinbase_pro import CoinbaseProAPI
from orderbook_builder import OrderbookBuilder, OrderbookSnapshotLoader
from tools.GracefulKiller import GracefulKiller
from depthchart import DepthChartPlotter
from tools.helper_tools import Timer
from websockets_coinbase import WebsocketClient, WebsocketClientHandler


# ======================================================================================
# Script Parameters

WEBHOOK_ONLY = False

DUMP_FEED_INTO_JSON = False
LOAD_FEED_FROM_JSON = False  # If true, no webhook

ITEM_DISPLAY_FLAGS = {
    "received": False,
    "open": False,
    "done": False,  # close orders
    "match": True,
    "change": False
}

BUILD_CANDLES = False
LOAD_ORDERBOOK_SNAPSHOT = True
PLOT_DEPTH_CHART = False

OUTPUT_FOLDER = 'data'

# for simulating feed
LOAD_FEED_FROM_JSON_FILEPATH = Path.cwd() / OUTPUT_FOLDER / "full_SNX-USD_dump_20220807-021730.json"

FREQUENCY = '1T'  # 1 min
# FREQUENCIES = ['1T', '5T', '15T', '1H', '4H', '1D']
SAVE_CSV = False
SAVE_HD5 = False  # Todo: Test this
SAVE_INTERVAL = 360
STORE_FEED_IN_MEMORY = False

# ======================================================================================
# Webhook Parameters

EXCHANGE = "Coinbase"
ENDPOINT = 'wss://ws-feed.exchange.coinbase.com'
MARKETS = ('ETH-USD',)
CHANNELS = ('full',)
# MARKETS = ('BTC-USD', 'ETH-USD', 'DOGE-USD', 'SHIB-USD', 'SOL-USD',
#           'AVAX-USD', 'UNI-USD', 'SNX-USD', 'CRV-USD', 'AAVE-USD', 'YFI-USD')

# ======================================================================================
# Configure logger

# # add file logger with full debug
# logger.add(
#     "logs\\coinbase_webhook_match_log_{time}.log", level="DEBUG"
# )

# add console logger with formatting
logger.add(
    sys.stdout, level="DEBUG",
    format="<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> --- <level>{level}</level> | Thread {thread} <level>{message}</level>"
)

# ======================================================================================


# windows only
# def ReopenDepthChart(title, text, style):
#     return ctypes.windll.user32.MessageBoxW(0, text, title, style)


def reopen_depth_chart():
    return easygui.ynbox("Reopen depth chart?", "Depth chart closed!")


def skip_finish_processing(data_qsize_cutoff: int):
    title = "Orderbook Builder wrapping up..."
    msg = f"More than {data_qsize_cutoff:,} pending items in orderbook queue.\n"
    msg += "Skip remaining items?"
    return easygui.ynbox(msg)


def main():
    # ensure 'data' output folder exists
    Path('data').mkdir(parents=True, exist_ok=True)

    data_queue = Queue()
    depth_chart_queue = Queue(maxsize=1)

    cb_api = CoinbaseAPI()  # used for authenticated websocket
    cbp_api = CoinbaseProAPI()  # used for rest API calls

    # load orderbook snapshot into queue
    snapshot_order_count = 0
    if LOAD_ORDERBOOK_SNAPSHOT:
        orderbook_snapshot = cbp_api.get_product_order_book(product_id=MARKETS[0], level=3)
        snapshot_loader = OrderbookSnapshotLoader(queue=data_queue, orderbook_snapshot=orderbook_snapshot)
        snapshot_order_count = snapshot_loader.order_count

    # handle websocket threads
    global LOAD_FEED_FROM_JSON_FILEPATH
    if not LOAD_FEED_FROM_JSON:
        LOAD_FEED_FROM_JSON_FILEPATH = None

        ws_handler = WebsocketClientHandler()

        for i, (market, channel) in enumerate(itertools.product(MARKETS, CHANNELS)):
            ws_handler.add(
                WebsocketClient(
                    api=cb_api,
                    channel=channel,
                    market=market,
                    data_queue=data_queue,
                    endpoint=ENDPOINT,
                    dump_feed=DUMP_FEED_INTO_JSON,
                    output_folder=OUTPUT_FOLDER,
                    module_timestamp=module_timestamp
                ),
                # wait until queue worker loads snapshot before piling on
                # Todo: test this further, since the delay between snapshot and websocket start will result in stale orderbook data
                start_immediately=True
            )

    # initialize plotter and queue_worker if not webhook-only mode
    if not WEBHOOK_ONLY:

        plotter = None
        if PLOT_DEPTH_CHART:
            plotter = DepthChartPlotter(title=f"Coinbase - {MARKETS[0]}", queue=depth_chart_queue)

        queue_worker = OrderbookBuilder(
            queue=data_queue,
            snapshot_order_count=snapshot_order_count,
            output_queue=depth_chart_queue,
            save_csv=SAVE_CSV,
            save_hd5=SAVE_HD5,
            save_interval=SAVE_INTERVAL,
            item_display_flags=ITEM_DISPLAY_FLAGS,
            build_candles=BUILD_CANDLES,
            load_feed_from_json_file=LOAD_FEED_FROM_JSON_FILEPATH,
            store_feed_in_memory=STORE_FEED_IN_MEMORY,
            module_timestamp=module_timestamp,
            module_timer=module_timer,
            exchange=EXCHANGE
        )

        queue_worker.thread.start()

        while queue_worker.queue_mode == "snapshot":
            time.sleep(0.1)

        if not ws_handler.start_signal_sent:
            ws_handler.start_all()

    reopen_prompt_flag = True  # flag that determines whether to reopen depth chart upon closing

    # keep main() from ending before threads are shut down, unless queue worker broke
    while not killer.kill_now:

        # run plotter
        if PLOT_DEPTH_CHART and not WEBHOOK_ONLY:

            if not plotter.closed:
                plotter.plot_depth_chart()

            else:

                if reopen_prompt_flag:
                    result = reopen_depth_chart()
                    if result:
                        plotter = DepthChartPlotter(title=f"Coinbase - {MARKETS[0]}", queue=depth_chart_queue)
                        # time.sleep(3)
                    else:
                        reopen_prompt_flag = False

        # send kill thread signal if queue worker breaks
        if not WEBHOOK_ONLY and not queue_worker.thread.is_alive():
            killer.kill_now = True

        if not ws_handler.all_threads_alive():
            logger.critical(f"Not all websocket threads alive!")
            killer.kill_now = True

    try:
        ws_handler.kill_all()
    except NameError:
        pass

    if not WEBHOOK_ONLY:
        queue_worker.finish()

        data_qsize_cutoff = 10000
        if data_queue.qsize() > data_qsize_cutoff:
            result = skip_finish_processing(data_qsize_cutoff)
            if result:
                queue_worker.stop()

        if queue_worker.thread.is_alive():
            queue_worker.thread.join()

        if PLOT_DEPTH_CHART:
            # logger.info(f"Remaining plotter queue size: {plotter.queue.qsize()}")
            plotter.queue.queue.clear()

        logger.info(f"Remaining data queue size: {data_queue.qsize()}")
        if data_queue.qsize() != 0:
            logger.warning("Queue worker did not finish processing the queue! Clearing all queues now...")
            data_queue.queue.clear()
            logger.info(f"Queues cleared.")


if __name__ == '__main__':
    module_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    module_timer = Timer()
    module_timer.start()
    killer = GracefulKiller()
    main()
    logger.info(f"Elapsed time = {module_timer.elapsed(hms_format=True)}")
