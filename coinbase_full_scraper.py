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

# homebrew modules
from api_coinbase import CoinbaseAPI
from api_coinbase_pro import CoinbaseProAPI
from websockets_coinbase import WebsocketClient, WebsocketClientHandler
from orderbook_builder import OrderbookBuilder, OrderbookSnapshotHandler
from tools.GracefulKiller import GracefulKiller
from tools.timer import Timer
import plotting.depth_chart_mpl as dpth
import plotting.performance_chart as perf

# ======================================================================================
# Script Parameters

WEBHOOK_ONLY = False

ENABLE_SNAPSHOT = True

SAVE_FEED = True
SAVE_ORDERBOOK_SNAPSHOT = True

LOAD_LOCAL_DATA = True  # If true, no webhook. Load orderbook snapshot and websocket feed from local files below
FEED_FILEPATH \
    = 'data/08-25-2022_Coinbase_ETH-USD/coinbase_full_ETH-USD_dump_20220825-220955.json.gz'
SNAPSHOT_FILEPATH \
    = 'data/08-25-2022_Coinbase_ETH-USD/coinbase_orderbook_snapshot_ETH-USD_34677807576_20220825-221013.json.gz'

ITEM_DISPLAY_FLAGS = {
    "received": False,
    "open": False,
    "done": False,  # close orders
    "match": True,
    "change": False
}
# Coinbase snapshot tends to provide a sequence earlier than where websocket starts
SNAPSHOT_GET_DELAY = 0.75  # in seconds.

ORDERBOOK_SNAPSHOT_DEPTH = 1000
BUILD_MATCHES = False
BUILD_CANDLES = False
PLOT_DEPTH_CHART = False

CANDLE_FREQUENCY = '1T'  # 1 min
# FREQUENCIES = ['1T', '5T', '15T', '1H', '4H', '1D']
SAVE_CSV = False
SAVE_INTERVAL = 360
STORE_FEED_IN_MEMORY = False

PLOT_PERFORMANCE = False
PERF_PLOT_INTERVAL = 0.05  # output to performance plotter queue every interval seconds
PERF_PLOT_WINDOW = 20  # in seconds (approximate)

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
    return easygui.ynbox(msg, title)


if __name__ == '__main__':
    logger.info("Starting orderbook builder!")
    module_timer = Timer()
    module_timer.start()
    module_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    killer = GracefulKiller()

    cb_api = CoinbaseAPI()  # used for authenticated websocket
    cbp_api = CoinbaseProAPI()  # used for rest API calls

    # ensure output folder exists
    OUTPUT_DIRECTORY = Path.cwd() / 'data' / f'{datetime.now().strftime("%m-%d-%Y")}_{EXCHANGE}_{MARKETS[0]}'
    if SAVE_CSV or SAVE_FEED or SAVE_ORDERBOOK_SNAPSHOT:
        OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    # main queue between websocket client and orderbook builder
    data_queue = queue.Queue()

    # start depth chart in separate process
    depth_chart_queue = None
    depth_chart_process = None
    if PLOT_DEPTH_CHART:
        # noinspection PyRedeclaration
        depth_chart_queue = mp.Queue(maxsize=1)
        # assert hasattr(depth_chart_queue, "_maxsize")
        args = (depth_chart_queue, )
        kwargs = {"title": f"{EXCHANGE} - {MARKETS[0]}", }
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
        if PERF_PLOT_INTERVAL == 0:
            window = int(PERF_PLOT_WINDOW / 0.01)
        else:
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
    if not LOAD_LOCAL_DATA:
        # noinspection PyRedeclaration
        ws_handler = WebsocketClientHandler()

        for i, (market, channel) in enumerate(itertools.product(MARKETS, CHANNELS)):
            ws_handler.add(
                WebsocketClient(
                    api=cb_api,
                    channel=channel,
                    market=market,
                    exchange=EXCHANGE,
                    data_queue=data_queue,
                    endpoint=ENDPOINT,
                    save_feed=SAVE_FEED,
                    output_folder=OUTPUT_DIRECTORY,
                    module_timer=module_timer,
                    stats_queue=perf_plot_queue,
                    stats_queue_interval=PERF_PLOT_INTERVAL,
                ),
                start_immediately=True
            )

    # load orderbook snapshot into queue
    snapshot_order_count = 0
    if not WEBHOOK_ONLY and ENABLE_SNAPSHOT:
        if SNAPSHOT_GET_DELAY != 0:
            logger.debug(f"Delaying orderbook snapshot request by {SNAPSHOT_GET_DELAY} seconds...")
            time.sleep(SNAPSHOT_GET_DELAY)

        orderbook_snapshot = None
        snapshot_filepath = None
        if not LOAD_LOCAL_DATA:
            # noinspection PyRedeclaration
            orderbook_snapshot = cbp_api.get_product_order_book(product_id=MARKETS[0], level=3)
        else:
            # noinspection PyRedeclaration
            snapshot_filepath = Path.cwd() / SNAPSHOT_FILEPATH

        snapshot_loader = OrderbookSnapshotHandler(
            queue=data_queue,
            depth=ORDERBOOK_SNAPSHOT_DEPTH,
            orderbook_snapshot=orderbook_snapshot,
            save=SAVE_ORDERBOOK_SNAPSHOT and not LOAD_LOCAL_DATA,
            market=MARKETS[0],
            save_folder=OUTPUT_DIRECTORY,
            snapshot_filepath=snapshot_filepath,
            exchange=EXCHANGE,
        )

        # noinspection PyRedeclaration
        snapshot_order_count = snapshot_loader.order_count

    # initialize depth_chart and queue_worker if not webhook-only mode
    orderbook_builder = None
    if not WEBHOOK_ONLY:
        if LOAD_LOCAL_DATA:
            load_feed_filepath = Path.cwd() / FEED_FILEPATH
        else:
            load_feed_filepath = None
        # noinspection PyRedeclaration
        orderbook_builder = OrderbookBuilder(
            queue=data_queue,
            snapshot_order_count=snapshot_order_count,
            output_queue=depth_chart_queue,
            save_csv=SAVE_CSV,
            save_interval=SAVE_INTERVAL,
            item_display_flags=ITEM_DISPLAY_FLAGS,
            build_matches=BUILD_MATCHES,
            build_candles=BUILD_CANDLES,
            load_feed_filepath=load_feed_filepath,
            store_feed_in_memory=STORE_FEED_IN_MEMORY,
            module_timer=module_timer,
            exchange=EXCHANGE,
            stats_queue=perf_plot_queue,
            stats_queue_interval=PERF_PLOT_INTERVAL,
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

        # send kill thread signal if orderbook builder breaks
        if orderbook_builder is not None and not orderbook_builder.thread.is_alive():
            killer.kill_now = True

        # send kill thread signal if any websockets break
        if ws_handler is not None and not ws_handler.all_threads_alive():
            logger.critical(f"Not all websocket threads alive!")
            killer.kill_now = True

        # send kill thread signal if orderbook builder finishes
        if orderbook_builder.queue_mode == "stop":
            killer.kill_now = True

        time.sleep(1)

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

        # wait for depth chart process to stop
        if depth_chart_process is not None:
            logger.debug(f"Joining depth chart plot process...")
            depth_chart_process.join()
            logger.debug("Depth chart process joined.")
            # if depth_chart_process.is_alive():
            #     depth_chart_process.terminate()
            #     logger.debug("Depth chart process terminated.")

        logger.info(f"Remaining data queue size: {data_queue.qsize()}")
        if data_queue.qsize() != 0:
            logger.warning("Orderbook builder did not finish processing the queue! Clearing all queues now...")
            data_queue.queue.clear()
            logger.info(f"Queues cleared.")

    # wait for stats chart process to stop
    if perf_plot_process is not None:
        logger.debug(f"Joining performance plot process...")
        perf_plot_process.join()
        logger.debug("Performance plot process joined.")
        # if perf_plot_process.is_alive():
        #     perf_plot_process.terminate()
        #     logger.debug("Performance plot process terminated.")

    # if performance plot or depth chart closed before main process,
    # script will hang at end because of QueueFeederThreads, so need to clear queues again.
    if perf_plot_queue is not None:
        while not perf_plot_queue.empty():
            perf_plot_queue.get()
        perf_plot_queue.close()

    if depth_chart_queue is not None:
        while not depth_chart_queue.empty():
            depth_chart_queue.get()
        depth_chart_queue.close()

    # logger.debug(f"perf_plot_queue.qsize() = {perf_plot_queue.qsize()}")
    # logger.debug(f"depth_chart_queue.qsize() = {depth_chart_queue.qsize()}")

    logger.info(f"Elapsed time = {module_timer.elapsed(_format='hms')}")
