import json
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue, Empty
from threading import Thread

from loguru import logger
from termcolor import colored

from orderbook import LimitOrderBook, Order
from tools.helper_tools import Timer, s_print
from worker_dataframes import MatchDataFrame, CandleDataFrame


class OrderbookSnapshotLoader:
    def __init__(self, queue: Queue, orderbook_snapshot: dict = None):
        self.load_orderbook_snapshot(orderbook_snapshot, queue)
        self.order_count = 0

    def load_orderbook_snapshot(self, orderbook_snapshot, _queue) -> None:
        logger.info(f"Loading orderbook snapshot into queue...")

        for bid in orderbook_snapshot["bids"]:
            item = {
                "price": bid[0],
                "remaining_size": bid[1],
                "order_id": bid[2],
                "type": "snapshot",
                "side": "buy",
                "sequence": orderbook_snapshot["sequence"]
            }
            _queue.put(item)
            self.order_count += 1

        for ask in orderbook_snapshot["asks"]:
            item = {
                "price": ask[0],
                "remaining_size": ask[1],
                "order_id": ask[2],
                "type": "snapshot",
                "side": "sell",
                "sequence": orderbook_snapshot["sequence"]
            }
            _queue.put(item)
            self.order_count += 1

        logger.debug(f"Loaded orderbook snapshot of size {self.order_count} into queue.")


class OrderbookBuilder:
    """Build limit orderbook from msgs in queue"""
    def __init__(
            self,
            queue: None | Queue = None,
            load_snapshot_items: int = 0,
            output_queue: None | Queue = None,
            save_csv: bool = False,
            save_hd5: bool = False,
            output_folder: str = 'data',
            save_interval: None | int = None,
            item_display_flags: None | dict = None,
            build_candles: bool = False,
            load_feed_from_json: bool = False,
            load_feed_from_json_file: None | Path = None,
            store_feed_in_memory: bool = False,
            module_timestamp: str = None,
            module_timer: Timer = None,
            exchange: str = "Coinbase"
    ):
        # queue worker options
        self.market = None
        self.save_CSV = save_csv
        self.save_HD5 = save_hd5
        self.output_folder = output_folder
        self.save_timer = Timer()
        self.save_interval = save_interval
        self.build_candles = build_candles
        self.load_feed_from_json = load_feed_from_json
        self.json_filepath = load_feed_from_json_file
        self.store_feed_in_memory = store_feed_in_memory

        # misc
        self.module_timestamp = module_timestamp
        self.module_timer = module_timer
        self.exchange = exchange

        self.item_display_flags = {
            "subscriptions": True, "received": True, "open": True, "done": True, "match": True, "change": True
        }

        if isinstance(item_display_flags, dict):
            for item_type in item_display_flags:
                self.item_display_flags[item_type] = item_display_flags[item_type]

        # data structures
        self.lob = LimitOrderBook()
        self.traders_active = defaultdict(set)
        self.traders_total = set()
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
        self.queue_stats_interval = 60  # seconds
        self.queue_stats = defaultdict(list)
        self.queue_empty_displayed = False

        self.finish_up = False  # end flag

        self.__lob_checked = False
        self.__lob_check_count = 0

        self.thread = Thread(target=self.process_queue)

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
        assert hasattr(self, 'in_data')
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
            self.queue_stats["delta"].append(delta)
            self.queue_stats["delta"] = self.queue_stats["delta"][-1000:]  # limit to 1000 measurements

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

    def save_orderbook_snapshot(self) -> None:
        # Todo: build a custom JSON encoder for orderbook class
        last_timestamp = datetime.strftime(self.last_timestamp, "%Y%m%d-%H%M%S")
        json_filename = f"coinbase_{self.market}_orderbook_snapshot_{last_timestamp}.json"
        json_filepath = Path.cwd() / self.output_folder / json_filename
        with open(json_filepath, 'w', encoding='UTF-8') as f:
            json.dump(self.lob, f, indent=4)
        logger.info(f"Saved orderbook snapshot into {json_filepath.name}")

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
                    self.__lob_checked = False

                if (self.save_CSV or self.save_HD5) and self.save_timer.elapsed() > self.save_interval:
                    logger.info(f"Time elapsed: {self.module_timer.elapsed(hms_format=True)}")
                    self.save_dataframes()
                    self.save_timer.reset()

                if self.queue_stats_timer.elapsed() > self.queue_stats_interval:
                    self.track_qsize()
                    self.queue_stats_timer.reset()

            except Empty:

                # show msg once every queue_stats_interval
                if not self.queue_empty_displayed:
                    logger.info(f"Queue empty...")
                    self.queue_empty_displayed = True
                if self.queue_stats_timer.elapsed() > self.queue_stats_interval:
                    logger.info(f"Queue empty...")
                    self.queue_stats_timer.reset()

                    # run orderbook checks no more than once every queue_stats_interval
                    # todo: use a different interval
                    if not self.__lob_checked:
                        logger.info(f"Checking orderbook validity...", end='')
                        self.lob.check()
                        self.__lob_checked = True
                        self.__lob_check_count += 1

            finally:

                # check on main thread -> wrap it up if main thread broke
                if not threading.main_thread().is_alive() and not self.finish_up:
                    logger.critical("Main thread is dead! Wrapping it up...")
                    self.finish_up = True

                if self.finish_up and self.queue.empty():
                    if self.save_CSV or self.save_HD5:
                        self.save_dataframes(final=True)
                        # self.save_orderbook_snapshot()
                        self.log_summary()
                    logger.info("Queue worker has finished.")
                    break

    def log_summary(self):
        logger.info("________________________________ Summary ________________________________")
        self.lob.log_details()
        logger.info(f"Total unique market-maker IDs encountered = {len(self.traders_total):,}")
        logger.info(f"Final count of unique market-maker IDs in orderbook = {len(self.traders_active):,}")
        logger.info(f"Matches processed = {self.matches.total_items}")
        logger.info(f"Candles generated = {self.candles.total_items}")

        if len(self.queue_stats["delta"]) != 0:
            # giving datetime.timedelta(0) as the start value makes sum work on tds
            # source: https://stackoverflow.com/questions/3617170/average-timedelta-in-list
            average_timedelta = sum(self.queue_stats["delta"], timedelta(0)) / len(self.queue_stats["delta"])
            logger.info(f"Average webhook-processing delay = {average_timedelta}")
            logger.info(f"LOB validity checks performed = {self.__lob_check_count}")
        logger.info(f"________________________________ Summary End ________________________________")

        # Todo: Add more

    def process_item(self, item: dict) -> None:

        # s_print(f"item = {item}")

        item_type, sequence, order_id, \
            side, size, remaining_size, \
            old_size, new_size, \
            price, timestamp, trader_id \
            = \
            item.get("type"), item.get("sequence"), item.get("order_id"), \
            item.get("side"), item.get("size"), item.get("remaining_size"), \
            item.get("old_size"), item.get("new_size"), \
            item.get("price"), item.get("time"), item.get("client_oid")

        is_bid = True if side == "buy" else False

        # item validity checks ---------------------------------------------------------
        valid_sequence, valid_received, valid_open, valid_done, valid_change = True, True, True, True, True

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
            case "change":
                if None in {item_type, order_id, timestamp, new_size}:
                    logger.info(f"Invalid change msg.")
                    valid_change = False

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

                self.traders_active[trader_id].add(order_id)
                self.traders_total.add(trader_id)

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

                # self.lob.display_bid_tree()
                # self.lob.display_ask_tree()
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
                for trader, orders in self.traders_active.items():
                    if order_id in orders:
                        self.traders_active[trader].remove(order_id)
                        if self.traders_active[trader] == set():
                            self.traders_active.pop(trader)
                        break

                # self.lob.display_bid_tree()
                # self.lob.display_ask_tree()
                # self.lob.check()
                self.output_depth_chart_data()

            # process order changes
            case "change" if valid_change and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=float(new_size),
                    price=None,
                    timestamp=timestamp,
                )

                if self.item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("CHANGE", 'cyan'), end=' ')
                    s_print(f"Order -- {side} {new_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                self.lob.process(order, action="change")

                # self.lob.display_bid_tree()
                # self.lob.display_ask_tree()
                # self.lob.check()
                self.output_depth_chart_data()

            # process trades
            case "match" if valid_sequence:
                self.matches.process_item(item, display_match=self.item_display_flags[item_type])
                if self.build_candles:
                    self.candles.process_item(item)

            case _ if not valid_sequence:
                logger.warning(f"Item below provided out of sequence (current={self.last_sequence}, provided={sequence})")
                logger.warning(f"{item}")
                logger.info("Skipping processing...")

            case _:
                logger.critical(f"Below item's type is unhandled!")
                logger.critical(f"{item}")
                raise ValueError("Unhandled msg type")

    def output_depth_chart_data(self):
        if self.output_queue is not None and self.output_queue.qsize() < self.output_queue.maxsize:
            timestamp = datetime.strptime(self.lob.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%m/%d/%Y-%H:%M:%S")
            bid_levels, ask_levels = self.lob.levels
            data = {
                "timestamp": timestamp,
                "sequence": self.last_sequence,
                "unique_traders": len(self.traders_active),
                "bid_levels": bid_levels,
                "ask_levels": ask_levels
            }
            # logger.debug(f"PLACING item {self.output_item_counter}: bid_levels {bid_levels}")
            # logger.debug(f"PLACING item {self.output_item_counter}: ask_levels {ask_levels}")
            self.output_queue.put(data)

    def display_subscription(self, item: dict):
        assert len(item.get("channels")) == 1
        assert len(item["channels"][0]["product_ids"]) == 1
        self.market = item['channels'][0]['product_ids'][0]
        line_output = f"Subscribed to {self.exchange}'s '{self.market}' channel "
        line_output += f"for {item['channels'][0]['product_ids'][0]}"
        s_print(colored(line_output, "yellow"))

    def finish(self) -> None:
        self.finish_up = True
        logger.info(f"Wrapping up the queue... Remaining items: {self.queue.qsize()}")
