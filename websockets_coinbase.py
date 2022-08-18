import json
import threading
import time
from pathlib import Path
from queue import Queue
from threading import Thread
from tools.helper_tools import s_print

from loguru import logger
from websocket import create_connection, WebSocketConnectionClosedException

from api_coinbase import CoinbaseAPI

class WebsocketClient:
    def __init__(
            self,
            api: CoinbaseAPI,
            channel: str,
            market: str,
            data_queue: Queue = None,
            endpoint: str = None,
            dump_feed: bool = False,
            output_folder: str = None,
            module_timestamp: str = None,

    ) -> None:
        self.channel = channel
        self.market = market
        self.data_queue = data_queue
        self.id = self.channel + '_' + self.market
        self.ws = None
        self.ws_url = endpoint
        self.user = api.get_user()
        self.thread = None
        self.thread_id = None
        self.running = None
        self.kill = False
        self.dump_feed = dump_feed
        self.output_folder = output_folder
        self.module_timestamp = module_timestamp


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
        if self.dump_feed:
            json_msgs = []
            json_filename = f"coinbase_{self.channel}_{self.market}_dump_{self.module_timestamp}.json"
            json_filepath = Path.cwd() / self.output_folder / json_filename

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

                    if self.dump_feed:
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

        if self.dump_feed:
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(json_msgs, f, indent=4)
                logger.info(f"Dumped feed into {json_filename}.")

        self.running = False

    def process_msg(self, msg: dict):
        self.data_queue.put(msg)
        return None

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
        logger.info(f"Killing all websocket threads...")
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
