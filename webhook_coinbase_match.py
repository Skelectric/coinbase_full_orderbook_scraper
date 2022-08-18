"""Open authenticated websocket(s) to Coinbase's MATCH channel."""
# Todo: Refactor functions out into separate modules
# Todo: Add additional error handling (?)
from loguru import logger
from termcolor import colored
import threading
from datetime import datetime
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
from api_coinbase import CoinbaseAPI
import signal
import time
import json
import queue
from tools.helper_tools import Timer
import os
import sys
import itertools
from collections import defaultdict

import pandas as pd

from worker_dataframes import MatchDataFrame, CandleDataFrame

pd.options.display.float_format = '{:.6f}'.format

# ======================================================================================
# Webhook Parameters

VIEW_ONLY = False

EXCHANGE = "Coinbase"
ENDPOINT = 'wss://ws-feed.exchange.coinbase.com'
MARKETS = ('BTC-USD',)
CHANNELS = ('matches', )
# MARKETS = ('BTC-USD', 'ETH-USD', 'DOGE-USD', 'SHIB-USD', 'SOL-USD',
#           'AVAX-USD', 'UNI-USD', 'SNX-USD', 'CRV-USD', 'AAVE-USD', 'YFI-USD')
FREQUENCY = '1T'  # 1 min
# FREQUENCIES = ['1T', '5T', '15T', '1H', '4H', '1D']
SAVE_CSV = True
SAVE_HD5 = False  # Todo: Test this
SAVE_INTERVAL = 360
STORE_FEED_IN_MEMORY = False
BUILD_CANDLES = True

# ======================================================================================
# Configure logger

# remove default loguru logger
logger.remove()

# add file logger with full debug
logger.add(
    "logs\\coinbase_webhook_match_log_{time}.log", level="DEBUG"
)

# add console logger with formatting
logger.add(
    sys.stdout, enqueue=True, level="DEBUG",
    format="<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> --- <level>{level}</level> | Thread {thread} <level>{message}</level>"
)

# ======================================================================================

def s_print(*args, **kwargs):
    """Thread-safe print function with colors."""
    s_print_lock = Lock()
    with s_print_lock:
        print(*args, **kwargs)


class GracefulKiller:
    """Help kill threads when stop called."""
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        if sys.platform == 'win32':
            signal.signal(signal.SIGBREAK, self.exit_gracefully)

    def exit_gracefully(self, *args):
        logger.critical("Stop signal sent. Stopping threads.............................")
        self.kill_now = True


class WebsocketClient:
    def __init__(self, api: CoinbaseAPI, channel: str, market: str, data_queue: queue.Queue) -> None:
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
        self.relevant_match_fields = ("type", "time", "trade_id", "product_id", "side", "size", "price",)

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
                    item = self.parse_msg(msg)
                    self.data_queue.put(item)
                else:
                    logger.warning("Webhook message is empty!")

        # close websocket
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException:
            pass

        logger.info(f"Closed websocket for {self.id}.")
        self.running = False

    def parse_msg(self, msg: dict):

        if "type" in msg and msg["type"] in {"last_match", "match"}:
            # _msg = {key: value for key, value in msg.items() if key in self.relevant_match_fields}
            # _msg["time"] = datetime.strptime(_msg.get('time'), "%Y-%m-%dT%H:%M:%S.%fZ")
            # self.display_match(_msg)
            # item = pd.DataFrame(columns=self.relevant_match_fields, data=[_msg])
            # return item
            return msg

        elif "type" in msg and msg["type"] == "subscriptions":

            line_output = f"Subscribed to {EXCHANGE}'s '{msg['channels'][0]['name']}' channel "
            line_output += f"for {msg['channels'][0]['product_ids'][0]}"
            s_print(colored(line_output, "yellow"))

        else:
            s_print(msg)

    def display_match(self, _msg) -> None:

        # build text
        line_output_0 = f"Thread {self.thread_id} - "
        line_output_1 = f"{_msg['time']} --- "
        line_output_2 = f"{_msg['side']} "
        line_output_3 = f"{_msg['size']} {_msg['product_id']} at ${float(_msg['price']):,}"

        # calc volume
        usd_volume = float(_msg['size']) * float(_msg['price'])
        line_output_right = f"${usd_volume:,.2f}"

        # handle colors
        line_output_0 = colored(line_output_0, "grey")
        if _msg["side"] == "buy":
            line_output_2 = colored(line_output_2, "green")
            line_output_right = colored(line_output_right, "green")
        elif _msg["side"] == "sell":
            line_output_2 = colored(line_output_2, "red")
            line_output_right = colored(line_output_right, "red")

        # alignment
        line_output_left = line_output_1 + line_output_2 + line_output_3
        line_output = f"{line_output_left:<80}{line_output_right:>25}"

        s_print(line_output)

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
                        logger.info(f"Pinged websocket for {websocket_client.__market}.")
                    except WebSocketConnectionClosedException:
                        pass
            time.sleep(interval)
        logger.info(f"No websockets open. Ending keepalive thread...")

    @property
    def short_str_markets(self) -> str:
        # get market symbols and append into single string i.e. BTC,ETH,SOL
        return ','.join([websocket.__market[:websocket.__market.find("-")] for websocket in self.websocket_clients])

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

    def __init__(self, _queue: queue.Queue) -> None:
        self.worker_dataframes = []
        self.queue = _queue
        self.delay = 2
        self.timer = Timer()
        self.save_timer = Timer()
        self.save_interval = SAVE_INTERVAL  # seconds
        self.queue_stats_timer = Timer()
        self.queue_stats_interval = 30  # seconds
        self.queue_stats = defaultdict(list)
        self.finish_up = False
        self.save_CSV = SAVE_CSV
        self.save_HD5 = SAVE_HD5
        self.store_df_in_mem = STORE_FEED_IN_MEMORY
        self.channels = CHANNELS
        self.build_candles = BUILD_CANDLES
        self.thread = Thread(target=self.process_queue)
        self.thread.start()

        if self.save_CSV or self.save_HD5:
            self.save = True
        else:
            self.save = False

    def track_qsize(self, _print: bool = True) -> None:
        self.queue_stats["time"].append(self.timer.elapsed())
        self.queue_stats["queue_sizes"].append(self.queue.qsize())
        avg_qsize = sum(self.queue_stats["queue_sizes"]) / len(self.queue_stats["queue_sizes"])
        elapsed_time = min(self.timer.elapsed(), 1000*self.save_interval)
        if _print:
            logger.info(f"Average queue size over {self.timer.elapsed(hms_format=True)} seconds: {avg_qsize:.2f}")
        self.queue_stats["avg_qsize"].append(avg_qsize)
        # keep track of the last 1000 queue sizes (so 10k seconds)
        self.queue_stats["queue_sizes"] = self.queue_stats["queue_sizes"][-1000:]
        self.queue_stats["avg_qsize"] = self.queue_stats["avg_qsize"][-1000:]

    def initialize_dataframes(self) -> None:
        self.worker_dataframes.append(MatchDataFrame(exchange="Coinbase", timestamp=timestamp))
        self.worker_dataframes.append(CandleDataFrame(exchange="Coinbase", frequency=FREQUENCY, timestamp=timestamp))

    def save_dataframes(self, final: bool = False) -> None:
        os.makedirs('data', exist_ok=True)  # ensure 'data' output folder exists
        for wdf in self.worker_dataframes:
            if not wdf.is_empty:
                if not final:
                    wdf.save_chunk()
                else:
                    wdf.save_chunk(update_filename_flag=True)
            if not self.store_df_in_mem:
                wdf.clear()

    def process_queue(self) -> None:
        self.timer.start()
        self.save_timer.start()
        self.queue_stats_timer.start()
        logger.info(f"Queue worker starting in {self.delay} seconds...")
        time.sleep(self.delay)
        logger.info(f"Queue worker starting now. Starting queue size: {self.queue.qsize()} items")

        self.initialize_dataframes()
        self.track_qsize()

        while True:
            try:
                item = self.queue.get(timeout=0.01)
                # logger.debug(f"item =\n{item}")

                if item is None:
                    continue

                try:
                    self.process_item(item)
                    # logger.debug(f"Item processed. Queue size: {data_queue.qsize()} items")

                finally:
                    self.queue.task_done()

                if self.save and self.save_timer.elapsed() > self.save_interval:
                    self.save_dataframes()
                    self.save_timer.reset()

                if self.queue_stats_timer.elapsed() > self.queue_stats_interval:
                    self.track_qsize(_print=True)
                    self.queue_stats_timer.reset()

            except queue.Empty:
                # logger.debug(f"Queue is Empty. Sleeping for {self.delay} seconds...")
                # time.sleep(self.delay)
                pass

            if self.finish_up:
                logger.info(f"Wrapping up the queue... Remaining items: {self.queue.qsize()}")

            if (self.save_CSV or self.save_HD5) and self.finish_up and queue.Empty:  # Perform final dataframe saves
                self.save_dataframes(final=True)
                break

        logger.info("Queue worker has finished.")

    def process_item(self, item: list) -> None:
        for wdf in self.worker_dataframes:
            wdf.process_item(item)

    def finish(self) -> None:
        self.finish_up = True


def main():

    data_queue = queue.Queue()

    # on Coinbase, enables authenticated match channel subscription
    api = CoinbaseAPI()

    # handle websocket threads
    ws_handler = WebsocketClientHandler()
    for i, (market, channel) in enumerate(itertools.product(MARKETS, CHANNELS)):
        ws_handler.add(WebsocketClient(api=api, channel=channel, market=market, data_queue=data_queue))

    if not VIEW_ONLY:
        queue_worker = QueueWorker(data_queue)

    # keep main() from ending before threads are shut down, unless queue worker broke
    while not killer.kill_now:
        time.sleep(1)
        if not VIEW_ONLY and not queue_worker.thread.is_alive():
            killer.kill_now = True

    ws_handler.kill_all()

    if not VIEW_ONLY:
        queue_worker.finish()

        if queue_worker.thread.is_alive():
            queue_worker.thread.join()

        logger.info(f"Remaining queue size: {data_queue.qsize()}")
        if data_queue.qsize() != 0:
            logger.warning("Queue worker did not finish processing the queue! Clearing it now...")
            data_queue.queue.clear()
            logger.info(f"Queue cleared.")


if __name__ == '__main__':
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    module_timer = Timer()
    module_timer.start()
    killer = GracefulKiller()
    main()
    module_timer.elapsed(display=True)
