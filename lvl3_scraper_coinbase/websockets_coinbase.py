import json
import gzip
import threading
import time
from pathlib import Path
import multiprocessing as mp
import queue
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from datetime import datetime, timedelta
from collections import deque
import numpy as np

# third party modules
from loguru import logger
from api_coinbase import CoinbaseAPI

# homebrew modules
from tools.helper_tools import s_print
from tools.timer import Timer
from tools.run_once_per_interval import run_once_per_interval, run_once
from plotting.performance import PerfPlotQueueItem

# ======================================================================================
import sys
# Platform specific imports and config
if sys.platform == "win32":
    from multiprocessing import Queue
elif sys.platform == 'darwin':
    from tools.mp_queue_OSX import Queue
# ======================================================================================


class WebsocketClient:
    def __init__(
            self,
            api: CoinbaseAPI,
            data_queue: queue.Queue = None,
            module_timer: Timer = None,
            stats_queue: Queue = None,
            stats_queue_interval: float = None,
            *args, **kwargs
    ) -> None:
        self.channel = kwargs.get("channel", None)
        self.market = kwargs.get("market", None)
        self.exchange = kwargs.get("exchange", None)
        self.data_queue = data_queue
        self.id = self.channel + '_' + self.market
        self.ws_url = kwargs.get("endpoint", None)
        self.user = api.get_user()
        self.save_feed = kwargs.get("save_feed", False)
        self.output_folder = kwargs.get("output_folder", "data")
        self.module_timer = module_timer

        self.ws = None
        self.thread = None
        self.thread_id = None
        self.running = None
        self.kill = False

        # performance monitoring
        self.latest_timestamp = None
        self.stats_queue = stats_queue
        self.stats_queue_interval = stats_queue_interval
        if self.stats_queue is not None:
            self.websocket_perf = PerfPlotQueueItem("websocket_thread", module_timer=module_timer)

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

        feed_filename = None
        if self.save_feed:
            module_timestamp = self.module_timer.get_start_time(_format="datetime_utc").strftime("%Y%m%d-%H%M%S")
            feed_filename = f"{self.exchange}_{self.channel}_{self.market}_dump_{module_timestamp}.json.gz"
            Path(self.output_folder).mkdir(parents=True, exist_ok=True)
            feed_filepath = Path.cwd() / self.output_folder / feed_filename
            f = gzip.open(feed_filepath, 'wt', encoding='UTF-8')

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
                # logger.debug(e)
                logger.debug(f"{e} - data: {feed}")
                break
            else:
                if msg != {}:

                    if self.save_feed:
                        f.write(json.dumps(msg)+'\n')

                    self.process_msg(msg)

                    self.__output_perf_data()

                else:
                    logger.warning("Webhook message is empty!")

        if self.save_feed:
            f.close()
            logger.debug(f"Saved feed into {feed_filename}")

        # signal to performance plotter that messages are ending
        self.__end_perf_data()

        # close websocket
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException:
            pass
        logger.info(f"Closed websocket for {self.id}.")

        self.running = False

    @run_once_per_interval("stats_queue_interval")
    def __output_perf_data(self):
        if self.stats_queue is not None:
            self.websocket_perf.send_to_queue(self.stats_queue)

    @run_once
    def __end_perf_data(self):
        if self.stats_queue is not None:
            self.websocket_perf.signal_end_item()
            self.websocket_perf.send_to_queue(self.stats_queue)
            logger.debug(f"Websocket sent 'None' to stats queue.")

    def process_msg(self, msg: dict):
        timestamp = msg.get("time")
        if timestamp is not None:
            self.latest_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
        else:
            self.latest_timestamp = datetime.utcnow().timestamp()
        self.data_queue.put(msg)
        if hasattr(self, "websocket_perf"):
            self.websocket_perf.track(timestamp=self.latest_timestamp)

    def start_thread(self) -> None:
        logger.info(f"Starting websocket thread for {self.id}....")
        self.thread = Thread(target=self.websocket_thread)
        self.thread.start()

    def kill_thread(self) -> None:
        logger.info(f"Closing websocket thread for {self.id}...")
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
        self.start_signal_sent = False

    def add(self, websocket_client: WebsocketClient, start_immediately: bool = True) -> None:
        self.websocket_clients.append(websocket_client)
        if start_immediately:
            self.start(websocket_client)

    def start(self, websocket_client) -> None:
        self.start_signal_sent = True
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
        # logger.info(f"Killing all websocket threads...")
        for websocket_client in self.websocket_clients:
            self.kill(websocket_client)
        time.sleep(1)
        self.check_finished()

    def all_threads_alive(self) -> bool:
        for websocket_client in self.websocket_clients:
            if not websocket_client.thread.is_alive():
                return False
        return True

    def check_finished(self) -> None:
        while len(self.get_active) != 0:
            i = 0
            while i < len(self.websocket_clients):
                if not self.websocket_clients[i].running:
                    # logger.debug(f"{self.websocket_clients[i].id} has stopped running.")
                    self.websocket_clients.remove(self.websocket_clients[i])
                    logger.info(f"{len(self.get_active)} websocket thread(s) remaining.")
                else:
                    logger.info(f"{self.websocket_clients[i].id} is still running.")
                    i += 1

            if len(self.get_active) != 0:
                logger.info(f"Checking again in 5 seconds.")
                time.sleep(5)

    def websocket_thread_keepalive(self, interval=60) -> None:
        time.sleep(interval//10)

        @run_once_per_interval(interval)
        def ping_all():
            for websocket_client in self.websocket_clients:
                if websocket_client.running:
                    try:
                        websocket_client.ping("keepalive")
                        logger.info(f"Pinged websocket for {websocket_client.market}.")
                    except WebSocketConnectionClosedException:
                        pass

        while self.websockets_open:
            ping_all()
            time.sleep(interval//10)

        logger.info(f"No websockets open. Ending keepalive thread...")

    @property
    def short_market_str(self) -> str:
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
        return [x.id for x in self.websocket_clients if x.thread.is_alive()]
