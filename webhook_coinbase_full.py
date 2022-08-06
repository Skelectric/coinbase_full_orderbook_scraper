"""Build orderbook using webhook to Coinbase's FULL channel."""
from loguru import logger
from termcolor import colored
import threading
from datetime import datetime
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
from api_coinbase import CoinbaseAPI
import time
import json
import queue
from helper_tools import Timer
import os
import sys
import itertools
from collections import defaultdict
from GracefulKiller import GracefulKiller
from orderbook import LimitOrderBook, Order

import matplotlib.pyplot as plt

import pandas as pd
import numpy as np

pd.options.display.float_format = '{:.6f}'.format

# ======================================================================================
# Script Parameters

WEBHOOK_ONLY = False

DUMP_FEED_INTO_JSON = False
LOAD_FEED_FROM_JSON = True  # If true, no webhook

JSON_FILEPATH = "data/full_SNX-USD_dump_20220802-134751.json"  # for testing only

# ======================================================================================
# Webhook Parameters

EXCHANGE = "Coinbase"
ENDPOINT = 'wss://ws-feed.exchange.coinbase.com'
MARKETS = ('SNX-USD',)
CHANNELS = ('full',)
# MARKETS = ('BTC-USD', 'ETH-USD', 'DOGE-USD', 'SHIB-USD', 'SOL-USD',
#           'AVAX-USD', 'UNI-USD', 'SNX-USD', 'CRV-USD', 'AAVE-USD', 'YFI-USD')

# ======================================================================================
# Configure logger

# remove default loguru logger
logger.remove()

# # add file logger with full debug
# logger.add(
#     "logs\\coinbase_webhook_match_log_{time}.log", level="DEBUG"
# )

# add console logger with formatting
logger.add(
    sys.stdout, enqueue=True, level="DEBUG",
    format="<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> --- <level>{level}</level> | Thread {thread} <level>{message}</level>"
)

# ======================================================================================

s_print_lock = Lock()


def s_print(*args, **kwargs):
    """Thread-safe print function with colors."""
    with s_print_lock:
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
            json_filepath = f"data/{self.channel}_{self.market}_dump_{module_timestamp}.json"

        self.running = True

        feed = None
        while not self.kill:
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
                    self.display_msg(msg)

                    # For testing purposes only! Delete later
                    if DUMP_FEED_INTO_JSON:
                        json_msgs.append(msg)

                    self.process_msg(msg)

                else:
                    logger.warning("Webhook message is empty!")

        # close websocket
        try:
            if self.ws:
                self.ws.close()

                if DUMP_FEED_INTO_JSON:
                    with open(json_filepath, 'w', encoding='utf-8') as f:
                        json.dump(json_msgs, f, indent=4)

        except WebSocketConnectionClosedException:
            pass

        logger.info(f"Closed websocket for {self.id}.")
        self.running = False

    def process_msg(self, msg: dict):
        self.data_queue.put(msg)
        return None
        # todo

    @staticmethod
    def display_msg(msg: dict):

        if "type" in msg and msg["type"] == "subscriptions":

            line_output = f"Subscribed to {EXCHANGE}'s '{msg['channels'][0]['name']}' channel "
            line_output += f"for {msg['channels'][0]['product_ids'][0]}"
            s_print(colored(line_output, "yellow"))

        else:
            s_print(msg)

        return msg

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
        print(f"Starting websocket threads for: {[x.id for x in self.websocket_clients]}")
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
                    logger.debug(f"{self.websocket_clients[i].id} has stopped running.")
                    self.websocket_clients.remove(self.websocket_clients[i])
                    logger.info(f"{len(self.get_active)} thread(s) remaining.")
                else:
                    logger.info(f"{self.websocket_clients[i].id} is still running.")
                    i += 1

            if len(self.get_active) != 0:
                logger.info(f"Checking again in 10 seconds.")
                time.sleep(10)

    def websocket_thread_keepalive(self, interval=60) -> None:
        time.sleep(interval)
        while self.websockets_open:
            logger.debug(f"Active websockets: {self.get_active}")
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
    def __init__(self, _queue: queue.Queue, output_queue: queue.Queue):
        self.lob = LimitOrderBook()
        self.last_sequence = 0

        self.queue = _queue
        self.delay = 2  # seconds

        self.output_queue = output_queue

        self.output_freq = 4  # output every 10 items
        self.output_counter = 0

        # queue stats
        self.timer = Timer()
        self.queue_stats_timer = Timer()
        self.queue_stats_interval = 30  # seconds
        self.queue_stats = defaultdict(list)

        self.finish_up = False  # end flag

        if LOAD_FEED_FROM_JSON:
            self.fill_queue_from_json()

        self.thread = Thread(target=self.process_queue)
        self.thread.start()

    def fill_queue_from_json(self):
        """Use when LOAD_FEED_FROM_JSON is True to build a queue from JSON file."""
        assert LOAD_FEED_FROM_JSON
        with open(JSON_FILEPATH, 'r') as f:
            data = json.load(f)
            for item in data:
                self.queue.put(item)

    def track_qsize(self, _print: bool = True) -> None:
        self.queue_stats["time"].append(self.timer.elapsed())
        self.queue_stats["queue_sizes"].append(self.queue.qsize())
        avg_qsize = sum(self.queue_stats["queue_sizes"]) / len(self.queue_stats["queue_sizes"])
        if _print:
            logger.info(f"Average queue size over {self.timer.elapsed(hms_format=True)} seconds: {avg_qsize:.2f}")
        self.queue_stats["avg_qsize"].append(avg_qsize)
        # keep track of the last 1000 queue sizes (so 10k seconds)
        self.queue_stats["queue_sizes"] = self.queue_stats["queue_sizes"][-1000:]
        self.queue_stats["avg_qsize"] = self.queue_stats["avg_qsize"][-1000:]

    def save_orderbook_snapshot(self, final: bool = False) -> None:
        pass
        # todo

    def process_queue(self) -> None:
        self.timer.start()
        self.queue_stats_timer.start()
        # logger.info(f"Queue worker starting in {self.delay} seconds...")
        # time.sleep(self.delay)
        # logger.info(f"Queue worker starting now. Starting queue size: {self.queue.qsize()} items")

        # self.track_qsize()

        while True:
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

                # # debugging
                # if self.queue.qsize() < 500:
                #     raise Exception

                if self.queue_stats_timer.elapsed() > self.queue_stats_interval:
                    self.track_qsize(_print=True)
                    self.queue_stats_timer.reset()

            except queue.Empty:
                logger.debug(f"Queue is Empty. Sleeping for {self.delay} seconds...")
                time.sleep(self.delay)
                pass

            if self.finish_up:
                logger.info(f"Wrapping up the queue... Remaining items: {self.queue.qsize()}")

            if self.finish_up and queue.Empty:
                break

        logger.info("Queue worker has finished.")

    def process_item(self, item: dict) -> None:

        print("------------------------------------------------------------------------")
        # print(f"item = {item}")

        item_type, sequence, order_id, \
            side, remaining_size, \
            price, timestamp \
            = \
            item.get("type"), item.get("sequence"), item.get("order_id"), \
            item.get("side"), item.get("remaining_size"), \
            item.get("price"), item.get("time")

        is_bid = True if side == "buy" else False

        # item validity checks
        valid_open = True
        if None in {item_type, order_id, side, remaining_size, price, timestamp}:
            valid_open = False

        valid_done = True
        if None in {item_type, order_id, timestamp}:
            valid_done = False
            
        valid_sequence = True
        if sequence is None or sequence <= self.last_sequence:
            valid_sequence = False
        else:
            self.last_sequence = sequence

        # process items
        match item_type:
            case "subscriptions":
                self.display_subscription(item)

            # process new orders & changes
            case "open" | "change" if valid_open and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=float(remaining_size),
                    price=float(price),
                    timestamp=timestamp
                )
                print(colored("OPEN", 'green'), end=' ')
                print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                self.lob.process(order, action="add")

                self.display_orderbook_info()
                self.lob.check()

            # process order cancels
            case "done" if valid_done and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=None,
                    price=None,
                    timestamp=timestamp,
                )
                print(colored("CLOSE", 'red'), end=' ')
                print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                popped_order = self.lob.process(order, action="remove")

                self.display_orderbook_info()
                self.lob.check()

            case "match" if valid_sequence:
                print("MATCH", end=' ')
                print(item)
                # Todo
                # self.last_sequence = item["sequence"]
                # order = Order(
                #     uid=item["maker_order_id"],
                #     is_bid=True if item["side"] == "buy" else False,
                #     size=item["size"],
                #     price=item["price"]
                # )
                # self.lob.process(order)

            case "received" if valid_sequence:
                print("RECEIVED", end=' ')
                print(item)
                # Todo
                
            case _ if not valid_sequence:
                print(f"Item below provided out of sequence (current={self.last_sequence}, provided={sequence})")
                print(f"{item}")
                print("Skipping processing...")

            case _:
                raise ValueError("Unhandled msg type")

    def display_orderbook_info(self):
        # Debugging
        # print("\n_________orderbook_________")
        # levels = self.lob.levels()
        # print(f"levels = {levels}")
        # orders = self.lob.orders
        # print(f"orders = {orders}")
        if self.lob.levels() is not None:
            # print(f"levels bid count = {len(levels['bids'])}")
            # print(f"levels ask count = {len(levels['asks'])}")

            top_level = self.lob.top_level
            # print(f"top level = {top_level}")

            # self.lob.display_bid_tree()
            # self.lob.display_ask_tree()
            # print()

            self.output_counter += 1
            if self.output_counter > self.output_freq and None not in top_level:
                self.output_counter = 0
                self.update_depth_chart_data()

    def update_depth_chart_data(self):
        # get bid_levels and ask_levels, place into numpy arrays, and sort
        bids = np.fromiter(self.lob.bid_levels.items(), dtype="f,f")
        asks = np.fromiter(self.lob.ask_levels.items(), dtype="f,f")
        bids.sort()
        asks.sort()

        # split bids into prices and sizes (reverse order for bids only)
        bid_prices = np.vectorize(lambda x: x[0])(bids[::-1])
        bid_sizes = np.vectorize(lambda x: x[1])(bids[::-1])
        ask_prices = np.vectorize(lambda x: x[0])(asks)
        ask_sizes = np.vectorize(lambda x: x[1])(asks)

        # calculate order depth
        bid_depth = np.cumsum(bid_sizes)
        ask_depth = np.cumsum(ask_sizes)

        # for bids only: zip, sort and split
        bid_depth_zip = np.fromiter(zip(bid_prices, bid_depth), dtype='f,f')
        bid_depth_zip.sort()
        bid_prices = np.vectorize(lambda x: x[0])(bid_depth_zip)
        bid_depth = np.vectorize(lambda x: x[1])(bid_depth_zip)

        # set x and y values
        x = np.concatenate((bid_prices, ask_prices))
        y = np.concatenate((bid_depth, ask_depth))

        self.send_output((x, y))

    def send_output(self, item):
        self.output_queue.put(item)

    @staticmethod
    def display_subscription(item: dict):
        assert len(item.get("channels")) == 1
        assert len(item["channels"][0]["product_ids"]) == 1
        line_output = f"Subscribed to {EXCHANGE}'s '{item['channels'][0]['name']}' channel "
        line_output += f"for {item['channels'][0]['product_ids'][0]}"
        s_print(colored(line_output, "yellow"))

    def finish(self) -> None:
        self.finish_up = True


def main():
    os.makedirs('data', exist_ok=True)  # ensure 'data' output folder exists

    data_queue = queue.Queue()

    # enables authenticated channel subscription
    api = CoinbaseAPI()

    # handle websocket threads
    if not LOAD_FEED_FROM_JSON:
        ws_handler = WebsocketClientHandler()
        for i, (market, channel) in enumerate(itertools.product(MARKETS, CHANNELS)):
            ws_handler.add(WebsocketClient(api=api, channel=channel, market=market, data_queue=data_queue))

    # if not WEBHOOK_ONLY:
    #     queue_worker = QueueWorker(data_queue)

    # # keep main() from ending before threads are shut down, unless queue worker broke
    # while not killer.kill_now:
    #     time.sleep(1)
    #     if not WEBHOOK_ONLY and not queue_worker.thread.is_alive():
    #         killer.kill_now = True

    # initialize plotting objects
    plotting_queue = queue.Queue()

    plt.ion()
    plt.style.use('dark_background')
    fig, ax = plt.subplots()
    fig.suptitle("Order depth")

    if not WEBHOOK_ONLY:
        queue_worker = QueueWorker(data_queue, plotting_queue)

    # keep main() from ending before threads are shut down, unless queue worker broke
    while not killer.kill_now:

        # run plotting functions
        if plotting_queue.empty():
            pass
        else:
            x, y = plotting_queue.get()
            plotting_queue.task_done()
            ax.step(x, y)
            fig.canvas.flush_events()
            fig.canvas.draw()
            ax.cla()

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

        logger.info(f"Remaining queue size: {data_queue.qsize()}")
        if data_queue.qsize() != 0:
            logger.warning("Queue worker did not finish processing the queue! Clearing it now...")
            data_queue.queue.clear()
            logger.info(f"Queue cleared.")


if __name__ == '__main__':
    module_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    module_timer = Timer()
    module_timer.start()
    killer = GracefulKiller()
    main()
    module_timer.elapsed(display=True)
