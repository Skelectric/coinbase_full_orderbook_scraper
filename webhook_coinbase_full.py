"""Build orderbook using webhook to Coinbase's FULL channel."""
from loguru import logger
logger.remove()  # remove default logger

from termcolor import colored
import threading
from datetime import datetime, timedelta
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
import time
import json
import queue
import sys
import itertools
from collections import defaultdict
from pathlib import Path

import pandas as pd
pd.options.display.float_format = '{:.6f}'.format

import matplotlib
matplotlib.use('TkAgg')

# homebrew modules
from api_coinbase import CoinbaseAPI
from orderbook import LimitOrderBook, Order
from GracefulKiller import GracefulKiller
from depthchart import DepthChartPlotter
from helper_tools import Timer
from worker_dataframes import MatchDataFrame, CandleDataFrame


# ======================================================================================
# Script Parameters

WEBHOOK_ONLY = False

DUMP_FEED_INTO_JSON = False
LOAD_FEED_FROM_JSON = False  # If true, no webhook

ITEM_DISPLAY_FLAGS = {
    "received": False,
    "open": False,
    "done": False,  # close orders
    "match": True
}

BUILD_CANDLES = True

PLOT_DEPTH_CHART = True

output_folder = 'data'

# for simulating feed
JSON_FILEPATH = "full_SNX-USD_dump_20220807-021730.json"

FREQUENCY = '1T'  # 1 min
# FREQUENCIES = ['1T', '5T', '15T', '1H', '4H', '1D']
SAVE_CSV = True
SAVE_HD5 = False  # Todo: Test this
SAVE_INTERVAL = 30
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
logger.add(
    "logs\\coinbase_webhook_match_log_{time}.log", level="DEBUG"
)

# add console logger with formatting
logger.add(
    sys.stdout, level="DEBUG",
    format="<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> --- <level>{level}</level> | Thread {thread} <level>{message}</level>"
)

# ======================================================================================

s_lock = Lock()


def s_print(*args, **kwargs):
    """Thread-safe print function with colors."""
    with s_lock:
        print(*args, **kwargs)


class WebsocketClient:
    def __init__(self, api: CoinbaseAPI, channel: str, market: str, data_queue: queue.Queue = None) -> None:
        self.channel = channel
        self.market = market
        self.data_queue = data_queue
        self.id = self.channel + '_' + self.market
        self.ws = None
        self.ws_url = ENDPOINT
        self.user = api.get_user
        self.thread = None
        self.thread_id = None
        self.running = None
        self.kill = False

    def websocket_thread(self) -> None:
        self.ws = create_connection(self.ws_url)
        self.thread_id = threading.current_thread().ident
        self.ws.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "Sec-WebSocket-Extensions": "permessage-deflate",
                    "user_id": self.user["legacy_id"],
                    "profile_id": self.user["id"],
                    "product_ids": [self.market],
                    "channels": [self.channel]
                }
            )
        )

        # For local testing
        if DUMP_FEED_INTO_JSON:
            json_msgs = []
            json_filename = f"coinbase_{self.channel}_{self.market}_dump_{module_timestamp}.json"
            json_filepath = Path.cwd() / output_folder / json_filename

        self.running = True

        feed = None
        while not self.kill and threading.main_thread().is_alive():
            try:
                feed = self.ws.recv()
                if feed:
                    msg = json.loads(feed)
                else:
                    msg = {}
            except (ValueError, KeyboardInterrupt, Exception) as e:
                logger.debug(e)
                logger.debug(f"{e} - data: {feed}")
                break
            else:
                if msg != {}:

                    if DUMP_FEED_INTO_JSON:
                        json_msgs.append(msg)

                    self.process_msg(msg)

                else:
                    logger.warning("Webhook message is empty!")

        # close websocket
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException:
            pass
        logger.info(f"Closed websocket for {self.id}.")

        if DUMP_FEED_INTO_JSON:
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(json_msgs, f, indent=4)
                logger.info(f"Dumped feed into {json_filename}.")

        self.running = False

    def process_msg(self, msg: dict):
        self.data_queue.put(msg)
        return None
        # todo

    def start_thread(self) -> None:
        logger.info(f"Starting websocket thread for {self.id}....")
        self.thread = Thread(target=self.websocket_thread)
        self.thread.start()

    def kill_thread(self) -> None:
        logger.info(f"Killing websocket thread for {self.id}...")
        self.kill = True

    def ping(self, string: str = "keepalive") -> None:
        self.ws.ping("keepalive")

    @property
    def is_running(self) -> bool:
        return self.running

    @property
    def kill_sent(self) -> bool:
        return self.kill


class WebsocketClientHandler:
    def __init__(self) -> None:
        self.websocket_clients = []
        # self.active = []
        self.websocket_keepalive = Thread(target=self.websocket_thread_keepalive, daemon=True)
        self.kill_signal_sent = False

    def add(self, websocket_client: WebsocketClient, start_immediately: bool = True) -> None:
        self.websocket_clients.append(websocket_client)
        if start_immediately:
            self.start(websocket_client)

    def start(self, websocket_client) -> None:
        websocket_client.start_thread()
        # self.active.append(websocket_client.id)
        if not self.websocket_keepalive.is_alive():  # start keepalive thread
            self.websocket_keepalive.start()

    def start_all(self) -> None:
        s_print(f"Starting websocket threads for: {[x.id for x in self.websocket_clients]}")
        for websocket_client in self.websocket_clients:
            self.start(websocket_client)

    def kill(self, websocket_client) -> None:
        websocket_client.kill_thread()
        self.kill_signal_sent = True

    def kill_all(self) -> None:
        logger.info(f"Killing all websocket threads...")
        for websocket_client in self.websocket_clients:
            self.kill(websocket_client)
        time.sleep(1)
        self.check_finished()

    def check_finished(self) -> None:
        while len(self.get_active) != 0:
            i = 0
            while i < len(self.websocket_clients):
                if not self.websocket_clients[i].running:
                    # logger.debug(f"{self.websocket_clients[i].id} has stopped running.")
                    self.websocket_clients.remove(self.websocket_clients[i])
                    logger.info(f"{len(self.get_active)} thread(s) remaining.")
                else:
                    logger.info(f"{self.websocket_clients[i].id} is still running.")
                    i += 1

            if len(self.get_active) != 0:
                logger.info(f"Checking again in 2 seconds.")
                time.sleep(2)

    def websocket_thread_keepalive(self, interval=60) -> None:
        time.sleep(interval)
        while self.websockets_open:
            # logger.debug(f"Active websockets: {self.get_active}")
            # for i, websocket_client in enumerate(self.websocket_clients):
            for websocket_client in self.websocket_clients:
                if websocket_client.running:
                    try:
                        websocket_client.ping("keepalive")
                        logger.info(f"Pinged websocket for {websocket_client.market}.")
                    except WebSocketConnectionClosedException:
                        pass
            time.sleep(interval)
        logger.info(f"No websockets open. Ending keepalive thread...")

    @property
    def short_str_markets(self) -> str:
        # get market symbols and append into single string i.e. BTC,ETH,SOL
        return ','.join([websocket.market[:websocket.market.find("-")] for websocket in self.websocket_clients])

    @property
    def websockets_open(self) -> bool:
        if self.websocket_clients:
            return True
        else:
            return False

    @property
    def get_active(self) -> list:
        return [x.id for x in self.websocket_clients]


class QueueWorker:
    """Build limit orderbook from msgs in queue"""
    def __init__(
            self, queue,
            output_queue=None,
            save_csv=True,
            save_hd5=False,
            save_interval=None,
            item_display_flags=None,
            build_candles=True,
            load_feed_from_json=False,
            json_filepath=None,
            store_feed_in_memory=False,
    ):
        # queue worker options
        self.save_CSV = save_csv
        self.save_HD5 = save_hd5
        self.save_timer = Timer()
        self.save_interval = save_interval
        self.build_candles = build_candles
        self.load_feed_from_json = load_feed_from_json
        self.json_filepath = json_filepath
        self.store_feed_in_memory = store_feed_in_memory

        self.item_display_flags = {
            "subscriptions": True, "received": True, "open": True, "done": True, "match": True
        }
        if isinstance(item_display_flags, dict):
            for item_type in item_display_flags:
                self.item_display_flags[item_type] = item_display_flags[item_type]

        # data structures
        self.lob = LimitOrderBook()
        self.traders = defaultdict(set)
        self.matches = MatchDataFrame(exchange="Coinbase", timestamp=module_timestamp)
        if build_candles:
            self.candles = CandleDataFrame(exchange="Coinbase", frequency="1T", timestamp=module_timestamp)
        self.last_sequence = 0
        self.last_timestamp = None
        self.queue = queue
        self.output_queue = output_queue

        # queue stats
        self.timer = Timer()
        self.queue_stats_timer = Timer()
        self.queue_stats_interval = 10  # seconds
        self.queue_stats = defaultdict(list)

        self.finish_up = False  # end flag

        self.thread = Thread(target=self.process_queue)
        # self.thread.start()

        if self.load_feed_from_json:
            self.in_data = None
            self.load_json()

    def load_json(self):
        assert self.load_feed_from_json
        logger.info(f"LOAD_FEED_FROM_JSON set to True. Loading JSON file into queue.")
        with open(self.json_filepath, 'r') as f:
            self.in_data = iter(json.load(f))

    def fill_queue_from_json(self):
        """Use when LOAD_FEED_FROM_JSON is True to build a queue from JSON file."""
        assert hasattr(self, 'JSON_data')
        item = next(self.in_data, None)
        if item is not None:
            # logger.debug("Placing item from JSON_data into queue.")
            self.queue.put(item)

    def track_qsize(self, average: bool = False) -> None:
        if average:
            self.queue_stats["time"].append(self.timer.elapsed())
            self.queue_stats["queue_sizes"].append(self.queue.qsize())
            avg_qsize = sum(self.queue_stats["queue_sizes"]) / len(self.queue_stats["queue_sizes"])
            logger.info(f"Average queue size over {self.timer.elapsed(hms_format=True)}: {avg_qsize:.2f}")
            self.queue_stats["avg_qsize"].append(avg_qsize)
            # keep track of the last 1000 queue sizes (so 10k seconds)
            self.queue_stats["queue_sizes"] = self.queue_stats["queue_sizes"][-1000:]
            self.queue_stats["avg_qsize"] = self.queue_stats["avg_qsize"][-1000:]
        else:
            delta = max(datetime.utcnow() - self.last_timestamp, timedelta(0))
            logger.info(f"Script is {delta} seconds behind. Queue size = {self.queue.qsize()}")

    def save_dataframes(self, final: bool = False) -> None:
        worker_dataframes = [self.matches, self.candles]
        for wdf in worker_dataframes:
            if not wdf.is_empty:
                if not final:
                    wdf.save_chunk()
                else:
                    wdf.save_chunk(update_filename_flag=True)
            if not self.store_feed_in_memory:
                wdf.clear()

    def save_orderbook_snapshot(self, final: bool = False) -> None:
        pass
        # todo

    def process_queue(self) -> None:
        self.timer.start()
        self.save_timer.start()
        self.queue_stats_timer.start()
        # logger.info(f"Queue worker starting in {self.delay} seconds...")
        # time.sleep(self.delay)
        logger.info(f"Queue worker starting now. Starting queue size: {self.queue.qsize()} items")

        while True:

            # simulate queue
            if self.load_feed_from_json:
                self.fill_queue_from_json()

            try:
                item = self.queue.get(timeout=0.01)
                # logger.debug(f"item =\n{item}")

                if item is None:
                    continue

                try:
                    self.process_item(item)
                    # logger.debug(f"Item processed. Queue size: {self.queue.qsize()} items")

                finally:
                    self.queue.task_done()

                if (self.save_CSV or self.save_HD5) and self.save_timer.elapsed() > self.save_interval:
                    self.save_dataframes()
                    self.save_timer.reset()

                if self.queue_stats_timer.elapsed() > self.queue_stats_interval:
                    logger.info(f"Time elapsed: {module_timer.elapsed(hms_format=True)}")
                    self.track_qsize()
                    self.queue_stats_timer.reset()

            except queue.Empty:
                # if queue is empty, take the opportunity to do an LOB check
                self.lob.check()
                pass

            # check on main thread -> wrap it up if main thread broke
            if not threading.main_thread().is_alive() and not self.finish_up:
                logger.critical("Main thread is dead! Clearing queues and ending.")
                self.queue.queue.clear()
                if self.output_queue is not None:
                    self.output_queue.queue.clear()
                logger.info("Queues cleared.")
                break

            if self.finish_up and self.queue.empty():
                if self.save_CSV or self.save_HD5:
                    self.save_dataframes(final=True)
                logger.info("Queue worker has finished.")
                break

    def process_item(self, item: dict) -> None:

        # s_print(f"item = {item}")

        item_type, sequence, order_id, \
            side, size, remaining_size, \
            price, timestamp, trader_id \
            = \
            item.get("type"), item.get("sequence"), item.get("order_id"), \
            item.get("side"), item.get("size"), item.get("remaining_size"), \
            item.get("price"), item.get("time"), item.get("client_oid")

        is_bid = True if side == "buy" else False

        # item validity checks ---------------------------------------------------------
        valid_sequence, valid_received, valid_open, valid_done = True, True, True, True

        if sequence is None or sequence <= self.last_sequence:
            valid_sequence = False
        else:
            self.last_sequence = sequence
            self.last_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

        match item_type:
            case "received":
                if None in {item_type, order_id, timestamp}:
                    logger.info(f"Invalid received msg: {item}")
                    valid_received = False
            case "open":
                if None in {item_type, order_id, side, remaining_size, price, timestamp}:
                    logger.info(f"Invalid open msg.")
                    valid_open = False
            case "done":
                if None in {item_type, order_id, timestamp}:
                    logger.info(f"Invalid done msg.")
                    valid_done = False

        # process items ----------------------------------------------------------------
        match item_type:
            case "subscriptions":
                if self.item_display_flags[item_type]:
                    self.display_subscription(item)

            # process received
            case "received" if valid_received and valid_sequence:
                if self.item_display_flags[item_type]:
                    s_print("RECEIVED", end=' ')
                    s_print(item)

                self.traders[trader_id].add(order_id)

            # process new orders
            case "open" if valid_open and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=float(remaining_size),
                    price=float(price),
                    timestamp=timestamp
                )

                if self.item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("OPEN", 'yellow'), end=' ')
                    s_print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                self.lob.process(order, action="add")

                # self.lob.check()
                self.output_depth_chart_data()

            # process order cancels
            case "done" if valid_done and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=None,
                    price=None,
                    timestamp=timestamp,
                )

                if self.item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("CLOSE", 'magenta'), end=' ')
                    s_print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                self.lob.process(order, action="remove")

                # find trader id associated with order and delete trader if no orders left
                for trader, orders in self.traders.items():
                    if order_id in orders:
                        self.traders[trader].remove(order_id)
                        if self.traders[trader] == set():
                            self.traders.pop(trader)
                        break

                # self.lob.check()
                self.output_depth_chart_data()

            # process trades
            case "match" if valid_sequence:
                self.matches.process_item(item, display_match=self.item_display_flags[item_type])
                if self.build_candles:
                    self.candles.process_item(item)

            case _ if not valid_sequence:
                s_print(f"Item below provided out of sequence (current={self.last_sequence}, provided={sequence})")
                s_print(f"{item}")
                s_print("Skipping processing...")

            case _:
                raise ValueError("Unhandled msg type")

    def output_depth_chart_data(self):
        if self.output_queue is not None and self.output_queue.qsize() < self.output_queue.maxsize:
            timestamp = datetime.strptime(self.lob.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%m/%d/%Y-%H:%M:%S")
            bid_levels, ask_levels = self.lob.levels
            data = {
                "timestamp": timestamp,
                "sequence": self.last_sequence,
                "unique_traders": len(self.traders),
                "bid_levels": bid_levels,
                "ask_levels": ask_levels
            }
            # logger.debug(f"PLACING item {self.output_item_counter}: bid_levels {bid_levels}")
            # logger.debug(f"PLACING item {self.output_item_counter}: ask_levels {ask_levels}")
            self.output_queue.put(data)

    @staticmethod
    def display_subscription(item: dict):
        assert len(item.get("channels")) == 1
        assert len(item["channels"][0]["product_ids"]) == 1
        line_output = f"Subscribed to {EXCHANGE}'s '{item['channels'][0]['name']}' channel "
        line_output += f"for {item['channels'][0]['product_ids'][0]}"
        s_print(colored(line_output, "yellow"))

    def finish(self) -> None:
        self.finish_up = True
        logger.info(f"Wrapping up the queue... Remaining items: {self.queue.qsize()}")


def main(*args, **kwargs):
    # ensure 'data' output folder exists
    Path('data').mkdir(parents=True, exist_ok=True)

    data_queue = queue.Queue()

    # enables authenticated channel subscription
    api = CoinbaseAPI()

    # handle websocket threads
    if not LOAD_FEED_FROM_JSON:
        ws_handler = WebsocketClientHandler()
        for i, (market, channel) in enumerate(itertools.product(MARKETS, CHANNELS)):
            ws_handler.add(WebsocketClient(api=api, channel=channel, market=market, data_queue=data_queue))

    # initialize plotter and queue_worker if not webhook-only mode
    if not WEBHOOK_ONLY:

        plotter = None
        if PLOT_DEPTH_CHART:
            plotter = DepthChartPlotter(f"Coinbase - {MARKETS[0]}")

        queue_worker = QueueWorker(
            queue=data_queue,
            output_queue=plotter.queue if plotter is not None else None,
            save_csv=SAVE_CSV,
            save_hd5=SAVE_HD5,
            save_interval=SAVE_INTERVAL,
            item_display_flags=ITEM_DISPLAY_FLAGS,
            build_candles=BUILD_CANDLES,
            load_feed_from_json=LOAD_FEED_FROM_JSON,
            json_filepath=JSON_FILEPATH,
            store_feed_in_memory=STORE_FEED_IN_MEMORY
        )

        queue_worker.thread.start()

    # keep main() from ending before threads are shut down, unless queue worker broke
    while not killer.kill_now:

        # run plotter
        if PLOT_DEPTH_CHART and not WEBHOOK_ONLY:
            plotter.plot()
        else:
            time.sleep(1)

        # send kill thread signal if queue worker breaks
        if not WEBHOOK_ONLY and not queue_worker.thread.is_alive():
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
    module_timer.elapsed(display=True)
