"""Build orderbook using webhook to Coinbase's FULL channel."""
# Todo: Build multi-market support

from loguru import logger

logger.remove()  # remove default logger

from datetime import datetime
import queue
import sys
import itertools
from pathlib import Path
import time
import easygui
import multiprocessing as mp
import pandas as pd
pd.options.display.float_format = '{:.6f}'.format

# homebrew modules
from api_coinbase import CoinbaseAPI
from api_coinbase_pro import CoinbaseProAPI
from websockets_coinbase import WebsocketClient, WebsocketClientHandler
from orderbook_builder import OrderbookBuilder, OrderbookSnapshotLoader
from tools.GracefulKiller import GracefulKiller
from tools.timer import Timer
import plotting.depth_chart_mpl as dpth
import plotting.performance_chart as perf


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
LOAD_ORDERBOOK_SNAPSHOT = True
ORDERBOOK_SNAPSHOT_DEPTH = 1000
BUILD_CANDLES = False
PLOT_DEPTH_CHART = True
OUTPUT_FOLDER = 'data'

# for simulating feed
LOAD_FEED_FROM_JSON_FILEPATH = Path.cwd() / OUTPUT_FOLDER / "full_SNX-USD_dump_20220807-021730.json"

CANDLE_FREQUENCY = '1T'  # 1 min
# FREQUENCIES = ['1T', '5T', '15T', '1H', '4H', '1D']
SAVE_CSV = False
SAVE_HD5 = False  # Todo: Test this
SAVE_INTERVAL = 360
STORE_FEED_IN_MEMORY = False

PLOT_PERFORMANCE = True
PERF_PLOT_INTERVAL = 0.01  # output to performance plotter queue every interval seconds
PERF_PLOT_WINDOW = 10  # in seconds (approximate)

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
logger_format = "<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> "
logger_format += "--- <level>{level}</level> | Thread {thread} <level>{message}</level>"
logger.add(
    sys.stdout, level="DEBUG",
    format=logger_format,
)

# ======================================================================================


def skip_finish_processing(_data_qsize_cutoff: int):
    title = "Orderbook Builder wrapping up..."
    msg = f"More than {_data_qsize_cutoff:,} pending items in orderbook queue.\n"
    msg += "Skip remaining items?"
    return easygui.ynbox(msg)


if __name__ == '__main__':
    logger.info("Starting orderbook builder!")
    module_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    module_timer = Timer()
    module_timer.start()
    killer = GracefulKiller()

    cb_api = CoinbaseAPI()  # used for authenticated websocket
    cbp_api = CoinbaseProAPI()  # used for rest API calls

    # ensure 'data' output folder exists
    Path('data').mkdir(parents=True, exist_ok=True)

    # main queue between websocket client and orderbook builder
    data_queue = queue.Queue()

    # multiprocessing manager Todo: figure out what this does
    process_mgr = mp.Manager()
    mgr_list = process_mgr.list()

    # start depth chart in separate process
    depth_chart_queue = None
    depth_chart_process = None
    if PLOT_DEPTH_CHART:
        # noinspection PyRedeclaration
        depth_chart_queue = mp.Queue(maxsize=1)
        # assert hasattr(depth_chart_queue, "_maxsize")
        args = (depth_chart_queue, )
        kwargs = {"title": f"Coinbase - {MARKETS[0]}", }
        # noinspection PyRedeclaration
        depth_chart_process = mp.Process(
            target=dpth.initialize_plotter,
            args=args,
            kwargs=kwargs,
            name="Depth-Chart Plotter",
            daemon=True
        )
        depth_chart_process.start()

    # start process speed chart in separate process
    perf_plot_queue = None
    perf_plot_process = None
    if PLOT_PERFORMANCE:
        window = int(PERF_PLOT_WINDOW / PERF_PLOT_INTERVAL)
        logger.debug(f"Setting performance plot window to {window} points.")
        # noinspection PyRedeclaration
        perf_plot_queue = mp.Queue()
        args = (perf_plot_queue, )
        kwargs = {"window": window, }
        # noinspection PyRedeclaration
        perf_plot_process = mp.Process(
            target=perf.initialize_plotter,
            args=args,
            kwargs=kwargs,
            name="Performance Plotter",
            daemon=True
        )
        perf_plot_process.start()

    # handle websocket threads
    ws_handler = None
    if not LOAD_FEED_FROM_JSON:
        LOAD_FEED_FROM_JSON_FILEPATH = None
        # noinspection PyRedeclaration
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
                    module_timestamp=module_timestamp,
                    timer_queue=perf_plot_queue,
                    timer_queue_interval=PERF_PLOT_INTERVAL,
                ),
                start_immediately=True
            )

    # Todo: have orderbook builder ignore all sequences before snapshot is complete, when in snapshot mode

    # load orderbook snapshot into queue
    snapshot_order_count = 0
    if LOAD_ORDERBOOK_SNAPSHOT:
        orderbook_snapshot = cbp_api.get_product_order_book(product_id=MARKETS[0], level=3)
        snapshot_loader = OrderbookSnapshotLoader(
            queue=data_queue,
            depth=ORDERBOOK_SNAPSHOT_DEPTH,
            orderbook_snapshot=orderbook_snapshot
        )
        # noinspection PyRedeclaration
        snapshot_order_count = snapshot_loader.order_count

    # initialize depth_chart and queue_worker if not webhook-only mode
    orderbook_builder = None
    if not WEBHOOK_ONLY:
        # noinspection PyRedeclaration
        orderbook_builder = OrderbookBuilder(
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
            exchange=EXCHANGE,
            timer_queue=perf_plot_queue,
            timer_queue_interval=PERF_PLOT_INTERVAL,
        )

        orderbook_builder.thread.start()

        while orderbook_builder.queue_mode == "snapshot":
            time.sleep(0.1)
            if killer.kill_now:
                orderbook_builder.stop()

        if ws_handler is not None and not ws_handler.start_signal_sent:
            ws_handler.start_all()

    reopen_prompt_flag = True  # flag that determines whether to reopen depth chart upon closing

    # keep main() from ending before threads are shut down, unless queue worker broke
    while not killer.kill_now:

        # send kill thread signal if queue worker breaks
        if orderbook_builder is not None and not orderbook_builder.thread.is_alive():
            killer.kill_now = True

        if ws_handler is not None and not ws_handler.all_threads_alive():
            logger.critical(f"Not all websocket threads alive!")
            killer.kill_now = True

        time.sleep(1)

    # wait for depth chart to stop
    if depth_chart_process is not None:
        # depth_chart_process.join(1)
        # logger.debug("Depth chart process joined.")
        depth_chart_process.terminate()
        logger.debug("Depth chart process terminated.")

    # wait for time chart to stop
    if perf_plot_process is not None:
        # perf_plot_process.join(1)
        # logger.debug("Performance plot process joined.")
        perf_plot_process.terminate()
        logger.debug("Performance plot process terminated.")

    if ws_handler is not None:
        ws_handler.kill_all()

    if orderbook_builder is not None:
        orderbook_builder.finish()

        data_qsize_cutoff = 10000
        if data_queue.qsize() > data_qsize_cutoff:
            result = skip_finish_processing(data_qsize_cutoff)
            if result:
                orderbook_builder.stop()

        if orderbook_builder.thread.is_alive():
            orderbook_builder.thread.join()

        if depth_chart_queue is not None:
            # logger.info(f"Remaining depth_chart queue size: {depth_chart.queue.qsize()}")
            # logger.debug(f"Clearing depth_chart_queue...")
            while not depth_chart_queue.empty():
                depth_chart_queue.get()
            # logger.debug(f"depth_chart_queue cleared.")

        logger.info(f"Remaining data queue size: {data_queue.qsize()}")
        if data_queue.qsize() != 0:
            logger.warning("Queue worker did not finish processing the queue! Clearing all queues now...")
            data_queue.queue.clear()
            logger.info(f"Queues cleared.")

    logger.info(f"Elapsed time = {module_timer.elapsed(hms_format=True)}")
