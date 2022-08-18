"""Build orderbook using webhook to Coinbase's FULL channel."""
# Todo: Build multi-market support
from loguru import logger


logger.remove()  # remove default logger

from datetime import datetime
from queue import Queue
import sys
import itertools
from pathlib import Path

import pandas as pd
pd.options.display.float_format = '{:.6f}'.format

# homebrew modules
from api_coinbase import CoinbaseAPI
from api_coinbase_pro import CoinbaseProAPI
from orderbook_builder import OrderbookSnapshotLoader, OrderbookBuilder
from websockets_coinbase import WebsocketClient, WebsocketClientHandler
from tools.GracefulKiller import GracefulKiller
from depthchart import DepthChartPlotter, ReopenDepthChart
from tools.helper_tools import Timer

# ======================================================================================
# Script Parameters

WEBHOOK_ONLY = False

DUMP_FEED_INTO_JSON = False
LOAD_FEED_FROM_JSON = True  # If true, no webhook

ITEM_DISPLAY_FLAGS = {
    "match": True,
}

BUILD_CANDLES = False
LOAD_ORDERBOOK_SNAPSHOT = False
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
                )
            )

    # initialize plotter and queue_worker if not webhook-only mode
    if not WEBHOOK_ONLY:

        plotter = None
        if PLOT_DEPTH_CHART:
            plotter = DepthChartPlotter(title=f"Coinbase - {MARKETS[0]}", queue=depth_chart_queue)

        queue_worker = OrderbookBuilder(
            queue=data_queue,
            load_snapshot_items=snapshot_order_count,
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

    reopen_prompt = True

    # keep main() from ending before threads are shut down, unless queue worker broke
    while not killer.kill_now:

        # run plotter
        if PLOT_DEPTH_CHART and not WEBHOOK_ONLY:

            if not plotter.closed:
                plotter.plot()

            else:

                if reopen_prompt:
                    result = ReopenDepthChart()
                    if result:
                        plotter = DepthChartPlotter(title=f"Coinbase - {MARKETS[0]}", queue=depth_chart_queue)
                        # time.sleep(3)
                    else:
                        reopen_prompt = False

        # send kill thread signal if queue worker breaks
        if not WEBHOOK_ONLY and not queue_worker.thread.is_alive():
            killer.kill_now = True

        if not LOAD_FEED_FROM_JSON and not ws_handler.all_threads_alive():
            logger.critical(f"Not all websocket threads alive!")
            killer.kill_now = True

    try:
        ws_handler.kill_all()
    except NameError:
        pass

    if not WEBHOOK_ONLY:
        queue_worker.finish()

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
