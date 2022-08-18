import json
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from threading import Thread

from loguru import logger
from termcolor import colored

from orderbook import LimitOrderBook, Order
from tools.helper_tools import Timer, s_print
from tools.run_once_per_interval import run_once_per_interval
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
    """Update limit orderbook from msgs in queue"""
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
            load_feed_from_json_file: None | Path = None,
            store_feed_in_memory: bool = False,
            module_timestamp: str = None,
            module_timer: Timer = None,
            exchange: str = "Coinbase"
    ):
        # queue worker options
        self.__market = None
        self.__load_snapshot_items = load_snapshot_items
        self.__save_CSV = save_csv
        self.__save_HD5 = save_hd5
        self.__output_folder = output_folder
        self.__save_timer = Timer()
        self._save_interval = save_interval
        self.__build_candles = build_candles
        self.__store_feed_in_memory = store_feed_in_memory

        # set item display options
        self.__item_display_flags = {
            "subscriptions": False, "received": False, "open": False, "done": False, "match": False, "change": False
        }

        if isinstance(item_display_flags, dict):
            for item_type in item_display_flags:
                self.__item_display_flags[item_type] = item_display_flags[item_type]

        # data structures
        self.lob = LimitOrderBook()
        self.matches = MatchDataFrame(exchange=exchange, timestamp=module_timestamp)
        self.candles = None
        if build_candles:
            self.candles = CandleDataFrame(exchange=exchange, frequency="1T", timestamp=module_timestamp)
        self.last_sequence = 0
        self.last_timestamp = None
        self.queue = queue
        self.output_queue = output_queue

        # misc
        self.exchange = exchange
        self.module_timer = module_timer

        # queue stats attributes
        self.__timer = Timer()
        self.__queue_empty_timer = Timer()
        self._queue_empty_interval = 10
        self.__queue_stats_timer = Timer()
        self._queue_stats_interval = 60  # seconds
        self.__queue_stats = defaultdict(list)
        self.__queue_empty_displayed = False

        # lob check attributes
        self.__lob_check_timer = Timer()
        self._lob_check_interval = 60  # seconds
        self.__lob_checked = False
        self.__lob_check_count = 0

        # queue processing modes, in order
        self.__finish_up = False  # end flag
        self.__queue_modes = iter(
            ("snapshot", "websocket", "finish", "stop")
        )
        self.queue_mode = None
        self.__next_queue_mode()

        # skip snapshot mode if 0 snapshot items loaded
        if load_snapshot_items == 0:
            logger.debug("No snapshot items.")
            self.__next_queue_mode()

        # main processing thread
        self.thread = Thread(target=self.__process_queue)

        # used for debugging
        if load_feed_from_json_file is not None:
            logger.info(f"Path passed to load_feed_from_json_file. Placing elements in JSON object into queue.")
            in_data = self.__load_iter_json(load_feed_from_json_file)
            self.__fill_queue_from_json(in_data, self.queue)

    def __next_queue_mode(self):
        self.queue_mode = next(self.__queue_modes)
        logger.debug(f"Queue processing mode set to '{self.queue_mode}'")

    @staticmethod
    def __load_iter_json(json_filepath: Path):
        with open(json_filepath, 'r') as f:
            return iter(json.load(f))

    @staticmethod
    def __fill_queue_from_json(in_data: iter, queue: Queue):
        """Use when LOAD_FEED_FROM_JSON is True to build a queue from iterable."""
        while True:
            item = next(in_data, None)
            if item is not None:
                queue.put(item)
            else:
                break

    def __get_queue_stats(self, average: bool = False, delay: bool = False) -> None:
        if average:
            self.__queue_stats["time"].append(self.__timer.elapsed())
            self.__queue_stats["queue_sizes"].append(self.queue.qsize())
            avg_qsize = sum(self.__queue_stats["queue_sizes"]) / len(self.__queue_stats["queue_sizes"])
            logger.info(f"Average queue size over {self.__timer.elapsed(hms_format=True)}: {avg_qsize:.2f}")
            self.__queue_stats["avg_qsize"].append(avg_qsize)
            # keep track of the last 1000 queue sizes (so 10k seconds)
            self.__queue_stats["queue_sizes"] = self.__queue_stats["queue_sizes"][-1000:]
            self.__queue_stats["avg_qsize"] = self.__queue_stats["avg_qsize"][-1000:]

        if delay and self.last_timestamp is not None:
            delta = max(datetime.utcnow() - self.last_timestamp, timedelta(0))
            logger.info(f"Script is {delta} seconds behind. Queue size = {self.queue.qsize()}")
            self.__queue_stats["delta"].append(delta)
            self.__queue_stats["delta"] = self.__queue_stats["delta"][-1000:]  # limit to 1000 measurements

        if not (average or delay):
            logger.info(f"Queue size = {self.queue.qsize()}")

    @run_once_per_interval("_queue_stats_interval")
    def __timed_get_queue_stats(self, *args, **kwargs):
        self.__get_queue_stats(*args, **kwargs)

    def __save_dataframes(self, final: bool = False) -> None:
        if not (self.__save_CSV or self.__save_HD5):
            return
        logger.info(f"Saving dataframes. Time elapsed: {self.module_timer.elapsed(hms_format=True)}")

        if isinstance(self.matches, MatchDataFrame) and not self.matches.is_empty:
            self.matches.save_chunk(csv=self.__save_CSV, update_filename_flag=final)
            if not self.__store_feed_in_memory:
                self.matches.clear()

        if isinstance(self.candles, CandleDataFrame) and not self.candles.is_empty:
            self.candles.save_chunk(csv=self.__save_CSV, update_filename_flag=final)
            if not self.__store_feed_in_memory:
                self.candles.clear()

    @run_once_per_interval("_save_interval")
    def __timed_save_dataframes(self, *args, **kwargs) -> None:
        self.__save_dataframes(*args, **kwargs)

    def __save_orderbook_snapshot(self) -> None:
        # Todo: build a custom JSON encoder for orderbook class
        last_timestamp = datetime.strftime(self.last_timestamp, "%Y%m%d-%H%M%S")
        json_filename = f"coinbase_{self.__market}_orderbook_snapshot_{last_timestamp}.json"
        json_filepath = Path.cwd() / self.__output_folder / json_filename
        with open(json_filepath, 'w', encoding='UTF-8') as f:
            json.dump(self.lob, f, indent=4)
        logger.info(f"Saved orderbook snapshot into {json_filepath.name}")

    def __save(self, timed: bool = False, *args, **kwargs) -> None:
        if timed:
            self.__timed_save_dataframes(*args, **kwargs)
        else:
            self.__save_dataframes(*args, **kwargs)
        # Todo: add __save_orderbook_snapshot

    @run_once_per_interval("_lob_check_interval")
    def __timed_lob_check(self) -> None:
        if not self.__lob_checked:
            logger.info(f"Checking orderbook validity...", end='')
            self.lob.check()
            self.__lob_checked = True
            logger.info(f"Orderbook checked.")
            self.__lob_check_count += 1

    @run_once_per_interval("_queue_empty_interval")
    def __timed_queue_empty_note(self) -> None:
        logger.info(f"Queue empty...")

    def __get_and_process_item(self):
        """Get item from queue and call process_item. Returns False if failure, True if success"""
        if not self.queue.empty():
            item = self.queue.get()
            if item is not None:
                try:
                    self.__process_item(item)
                except Exception as e:
                    logger.critical(e)
                finally:
                    # self.queue.task_done()
                    self.__lob_checked = False
            else:
                logger.debug(f"item is none")
        else:
            self.__timed_queue_empty_note()
            self.__timed_lob_check()

    def __check_main_thread(self):
        """Check on main thread and choose next queue mode if main thread broke."""
        if not threading.main_thread().is_alive():
            logger.critical("Main thread is dead! Wrapping it up...")
            # skips to the last queue mode
            *_, self.queue_mode = self.__queue_modes

    def __check_snapshot_done(self):
        if self.lob.items_processed == self.__load_snapshot_items:
            logger.info(f"Snapshot processed.")
            self.__next_queue_mode()

    def finish(self):
        self.__next_queue_mode()
        logger.info(f"Wrapping up the queue... Remaining items: {self.queue.qsize()}")

    def __check_finished(self):
        if self.queue.empty():
            self.__next_queue_mode()

    def __clear_queues_and_exit(self):
        if not self.queue.empty():
            logger.info(f"Queue is not empty. Clearing remaining {self.queue.qsize()} items and exiting thread.")
            self.queue.queue.clear()
        else:
            logger.info("Queue is empty. Exiting thread.")

    def __process_queue(self) -> None:
        self.__timer.start()
        self.__lob_check_timer.start()
        self.__save_timer.start()
        self.__queue_stats_timer.start()
        self.__queue_empty_timer.start()
        logger.info(f"Queue worker starting now in {self.queue_mode} mode. Starting queue size: {self.queue.qsize()} items")

        while True:

            self.__check_main_thread()

            match self.queue_mode:

                case "snapshot":
                    self.__get_and_process_item()
                    self.__timed_get_queue_stats()
                    self.__check_snapshot_done()

                case "websocket":
                    self.__get_and_process_item()
                    self.__timed_get_queue_stats(delay=True)
                    self.__save(timed=True)

                case "finish":
                    self.__get_and_process_item()
                    self.__timed_get_queue_stats(delay=True)
                    self.__check_finished()

                case "stop":
                    self.__save_dataframes(final=True)
                    self.__log_summary()
                    self.__clear_queues_and_exit()
                    logger.info(f"Queue worker has finished.")
                    break

    def __log_summary(self):
        logger.info("________________________________ Summary ________________________________")
        self.lob.log_details()
        logger.info(f"Matches processed = {self.matches.total_items}")
        if self.__build_candles:
            logger.info(f"Candles generated = {self.candles.total_items}")

        if len(self.__queue_stats["delta"]) != 0:
            # giving datetime.timedelta(0) as the start value makes sum work on tds
            # source: https://stackoverflow.com/questions/3617170/average-timedelta-in-list
            average_timedelta = sum(self.__queue_stats["delta"], timedelta(0)) / len(self.__queue_stats["delta"])
            logger.info(f"Average webhook-processing delay = {average_timedelta}")
            logger.info(f"LOB validity checks performed = {self.__lob_check_count}")
        logger.info(f"________________________________ Summary End ________________________________")

        # Todo: Add more

    def __process_item(self, item: dict) -> None:

        # s_print(f"item = {item}")

        item_type, sequence, order_id, \
            side, size, remaining_size, \
            old_size, new_size, \
            price, timestamp, client_oid \
            = \
            item.get("type"), item.get("sequence"), item.get("order_id"), \
            item.get("side"), item.get("size"), item.get("remaining_size"), \
            item.get("old_size"), item.get("new_size"), \
            item.get("price"), item.get("time"), item.get("client_oid")

        is_bid = True if side == "buy" else False

        # item validity checks ---------------------------------------------------------
        valid_sequence, valid_received, valid_open, valid_done, valid_change = True, True, True, True, True

        if item_type != "snapshot" and (sequence is None or sequence <= self.last_sequence):
            valid_sequence = False
        else:
            self.last_sequence = sequence
            if item_type != "snapshot":
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
                if self.__item_display_flags[item_type]:
                    self.__display_subscription(item)

            # process received
            case "received" if valid_received and valid_sequence:
                if self.__item_display_flags[item_type]:
                    s_print("RECEIVED", end=' ')
                    s_print(item)

            # process new orders
            case "open" | "snapshot" if valid_open and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=float(remaining_size),
                    price=float(price),
                    timestamp=timestamp
                )

                if self.__item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("OPEN", 'yellow'), end=' ')
                    s_print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                self.lob.process(order, action="add")

                # self.lob.display_bid_tree()
                # self.lob.display_ask_tree()
                # self.lob.check()
                self.__output_depth_chart_data()

            # process order cancels
            case "done" if valid_done and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=None,
                    price=None,
                    timestamp=timestamp,
                )

                if self.__item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("CLOSE", 'magenta'), end=' ')
                    s_print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                self.lob.process(order, action="remove")

                # self.lob.display_bid_tree()
                # self.lob.display_ask_tree()
                # self.lob.check()
                self.__output_depth_chart_data()

            # process order changes
            case "change" if valid_change and valid_sequence:
                order = Order(
                    uid=order_id,
                    is_bid=is_bid,
                    size=float(new_size),
                    price=None,
                    timestamp=timestamp,
                )

                if self.__item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("CHANGE", 'cyan'), end=' ')
                    s_print(f"Order -- {side} {new_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                self.lob.process(order, action="change")

                # self.lob.display_bid_tree()
                # self.lob.display_ask_tree()
                # self.lob.check()
                self.__output_depth_chart_data()

            # process trades
            case "match" if valid_sequence:
                self.matches.process_item(item, display_match=self.__item_display_flags[item_type])
                if self.__build_candles:
                    self.candles.process_item(item)

            case _ if not valid_sequence:
                logger.warning(f"Item below provided out of sequence (current={self.last_sequence}, provided={sequence})")
                logger.warning(f"{item}")
                logger.info("Skipping processing...")

            case _:
                logger.critical(f"Below item's type is unhandled!")
                logger.critical(f"{item}")
                raise ValueError("Unhandled msg type")

    def __output_depth_chart_data(self):
        if self.output_queue is not None and self.output_queue.qsize() < self.output_queue.maxsize:
            timestamp = datetime.strptime(self.lob.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%m/%d/%Y-%H:%M:%S")
            bid_levels, ask_levels = self.lob.levels
            data = {
                "timestamp": timestamp,
                "sequence": self.last_sequence,
                "bid_levels": bid_levels,
                "ask_levels": ask_levels
            }
            # logger.debug(f"PLACING item {self.output_item_counter}: bid_levels {bid_levels}")
            # logger.debug(f"PLACING item {self.output_item_counter}: ask_levels {ask_levels}")
            self.output_queue.put(data)

    def __display_subscription(self, item: dict):
        assert len(item.get("channels")) == 1
        assert len(item["channels"][0]["product_ids"]) == 1
        self.__market = item['channels'][0]['product_ids'][0]
        line_output = f"Subscribed to {self.exchange}'s '{self.__market}' channel "
        line_output += f"for {item['channels'][0]['product_ids'][0]}"
        s_print(colored(line_output, "yellow"))
