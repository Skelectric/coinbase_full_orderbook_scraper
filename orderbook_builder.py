import json
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
import queue as q
import multiprocessing as mp
from threading import Thread
from itertools import islice, cycle
import copy

from loguru import logger
from termcolor import colored

from orderbook import LimitOrderBook, Order
from tools.helper_tools import s_print
from tools.timer import Timer
from tools.run_once_per_interval import run_once_per_interval, run_once
from worker_dataframes import MatchDataFrame, CandleDataFrame

import numpy as np


class JustContinueException(Exception):
    pass

# Todo: make an OrderbookSnapshotSaver


class OrderbookSnapshotLoader:
    """class that loads dict containing orderbook snapshot into the OrderbookBuilder's input queue"""
    def __init__(self, queue: q.Queue, depth: int = None, orderbook_snapshot: dict = None):
        self.order_count = 0
        self.sequence = orderbook_snapshot["sequence"]
        self.load_orderbook_snapshot(queue, orderbook_snapshot, depth)

    def load_orderbook_snapshot(self, _queue, orderbook_snapshot, depth) -> None:
        msg = "Loading orderbook snapshot into queue. "
        if depth is not None:
            msg += f"Ignoring orders more than {depth} away from best bid/ask."

        logger.debug(msg)

        bids = list(islice(orderbook_snapshot["bids"], depth))
        asks = list(islice(orderbook_snapshot["asks"], depth))

        best_bid, *_, worst_bid = bids
        best_ask, *_, worst_ask = asks

        higher = float(worst_ask[0]) / float(best_ask[0]) - 1
        lower = 1 - float(worst_bid[0]) / float(best_bid[0])

        logger.debug(f"Excluding orders {higher:.1%} higher than best ask and {lower:.1%} lower than best bid.")

        for bid in bids:
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

        for ask in asks:
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
    # Todo: replace with kwargs
    def __init__(
            self,
            queue: q.Queue = None,
            snapshot_order_count: int = 0,
            output_queue: mp.Queue = None,
            save_csv: bool = False,
            save_hd5: bool = False,
            output_folder: str = 'data',
            save_interval: int = None,
            item_display_flags: dict = None,
            build_candles: bool = False,
            load_feed_from_json_file: Path = None,
            store_feed_in_memory: bool = False,
            module_timer: Timer = None,
            exchange: str = "Coinbase",
            stats_queue: mp.Queue = None,
            stats_queue_interval: float = None,
    ):
        # queue worker options
        self.__market = None
        self.__snapshot_order_count = snapshot_order_count
        self.__save_CSV = save_csv
        self.__save_HD5 = save_hd5
        self.__output_folder = output_folder
        self.__save_timer = Timer()
        self._save_interval = save_interval
        self.__build_candles = build_candles
        self.__json_filepath = load_feed_from_json_file
        self.__store_feed_in_memory = store_feed_in_memory

        # misc
        self.module_timer = module_timer
        self.exchange = exchange

        self.__item_display_flags = {
            "subscriptions": False,
            "received": False,
            "open": False,
            "done": False,
            "match": False,
            "change": False,
            "snapshot": False
        }

        if isinstance(item_display_flags, dict):
            for item_type in item_display_flags:
                self.__item_display_flags[item_type] = item_display_flags[item_type]

        # data structures
        self.lob = LimitOrderBook()
        module_timestamp = self.module_timer.get_start_time(_format="datetime").strftime("%Y%m%d-%H%M%S")
        self.matches = MatchDataFrame(exchange="Coinbase", timestamp=module_timestamp)
        self.candles = None
        if build_candles:
            self.candles = CandleDataFrame(exchange="Coinbase", frequency="1T", timestamp=module_timestamp)
        self.first_sequence = None
        self.snapshot_sequence = None
        self.websocket_sequence = None
        self.missing_sequences = []
        self.prev_sequence = None
        self.sequence = None
        self.latest_timestamp = None
        self.queue = queue
        self.temp_queue = q.Queue()

        self.output_queue = output_queue
        if self.output_queue is not None:
            # q.Queue has maxsize, whereas mp.Queue has _maxsize
            assert hasattr(self.output_queue, '_maxsize')

        # queue stats
        self.__timer = Timer()
        self.__queue_empty_timer = Timer()
        self._queue_empty_interval = 60
        self.__queue_stats_timer = Timer()
        self._queue_stats_interval = 15  # seconds
        self.__queue_stats = defaultdict(list)
        self.__total_queue_items_processed = 0
        self.__total_queue_items_skipped = 0
        self.__prev_qsize = 0
        self.__last_timestamp = datetime.utcnow()

        # lob check attributes
        self.__lob_check_timer = Timer()
        self._lob_check_interval = 60  # seconds
        self.__lob_checked = True
        self.__lob_check_count = 0

        # queue processing modes, in order
        self._snapshot_stats_interval = 10
        # Todo: use a linked data structure for these queue modes
        # Todo: where each mode links to the next
        # Todo: and each subsequent nodes is aware of all previous nodes
        self.__queue_modes = ("snapshot", "websocket", "finish", "stop")
        self.__queue_modes_cycle = cycle(self.__queue_modes)
        self.queue_mode = None
        self.__next_queue_mode()

        # skip snapshot mode if 0 snapshot items loaded
        if snapshot_order_count == 0:
            logger.debug("No snapshot items.")
            self.__next_queue_mode()

        # main processing thread
        self.thread = Thread(target=self.__process_queue)

        # used for debugging
        if load_feed_from_json_file is not None:
            logger.info(f"Path passed to load_feed_from_json_file. Placing elements in JSON object into queue.")
            in_data = self.__load_iter_json(load_feed_from_json_file)
            self.__fill_queue_from_json(in_data, self.queue)

        # performance monitoring
        self.total_count = 0
        self.count = 0
        self.delays = deque()
        self.stats_queue = stats_queue
        self.stats_queue_interval = stats_queue_interval

    def __next_queue_mode(self):
        self.queue_mode = next(self.__queue_modes_cycle)
        logger.debug(f"Queue processing mode set to '{self.queue_mode}'")

    def __skip_to_next_queue_mode(self, mode: str):
        assert mode in self.__queue_modes
        mode_index = self.__queue_modes.index(mode)
        current_index = self.__queue_modes.index(self.queue_mode)
        if current_index < mode_index:
            while self.queue_mode != mode:
                self.__next_queue_mode()

    def __cycle_to_queue_mode(self, mode: str):
        """Similar to __skip_to_next_queue_mode but allows for setting mode back to snapshot."""
        assert mode in self.__queue_modes
        # Todo

    def finish(self):
        self.__skip_to_next_queue_mode("finish")
        logger.info(f"Wrapping up the queue... Remaining items: {self.queue.qsize()}")

    def stop(self):
        self.__skip_to_next_queue_mode("stop")
        logger.debug(f"Stop called. Skipping any remaining items in queue.")

    @staticmethod
    def __load_iter_json(json_filepath: Path):
        with open(json_filepath, 'r') as f:
            return iter(json.load(f))

    @staticmethod
    def __fill_queue_from_json(in_data: iter, queue: q.Queue):
        """Use when LOAD_FEED_FROM_JSON is True to build a queue from iterable."""
        while True:
            item = next(in_data, None)
            if item is not None:
                queue.put(item)
            else:
                break

    def __get_queue_stats(
            self, track_average: bool = False, track_delay: bool = False,
            snapshot_mode: bool = False) -> None:

        msg = ''

        if track_average:
            self.__queue_stats["time"].append(self.__timer.elapsed())
            self.__queue_stats["queue_sizes"].append(self.queue.qsize())
            avg_qsize = sum(self.__queue_stats["queue_sizes"]) / len(self.__queue_stats["queue_sizes"])
            logger.info(f"Average queue size over {self.__timer.elapsed(hms_format=True)}: {avg_qsize:.2f}")
            self.__queue_stats["avg_qsize"].append(avg_qsize)
            # keep track of the last 1000 queue sizes (so 10k seconds)
            self.__queue_stats["queue_sizes"] = self.__queue_stats["queue_sizes"][-1000:]
            self.__queue_stats["avg_qsize"] = self.__queue_stats["avg_qsize"][-1000:]

        if track_delay and self.latest_timestamp is not None:
            delta = max(datetime.utcnow() - self.latest_timestamp, timedelta(0))
            logger.info(f"Script is {delta} seconds behind.")
            self.__queue_stats["delta"].append(delta)
            self.__queue_stats["delta"] = self.__queue_stats["delta"][-1000:]  # limit to 1000 measurements

        if snapshot_mode:
            snapshot_orders = self.__snapshot_order_count - self.lob.items_processed
            msg += f"Snapshot orders left = {snapshot_orders}. "

        msg += f"Queue size = {self.queue.qsize()}."
        logger.info(msg)

        delta = datetime.utcnow() - self.__last_timestamp
        # logger.debug(delta.total_seconds())
        if delta.total_seconds() > 0.1:
            dones_per_sec = self.count // delta.total_seconds()
            logger.info(f"Items processed per second = {dones_per_sec:,}")
            self.__last_timestamp = datetime.utcnow()

    @run_once_per_interval("_queue_stats_interval")
    def __timed_get_queue_stats(self, *args, **kwargs):
        self.__get_queue_stats(*args, **kwargs)

    @run_once_per_interval("_snapshot_stats_interval")
    def __track_snapshot_load(self):
        self.__get_queue_stats(snapshot_mode=True)

    def __save_dataframes(self, final: bool = False) -> None:
        if not (self.__save_CSV or self.__save_HD5):
            return
        logger.info(f"Saving dataframes. Time elapsed: {self.module_timer.elapsed(_format='hms')}")

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

    def save_orderbook_snapshot(self) -> None:
        # Todo: build a custom JSON encoder for orderbook class
        last_timestamp = datetime.strftime(self.latest_timestamp, "%Y%m%d-%H%M%S")
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

    def __check_main_thread(self):
        """Check on main thread and choose next queue mode if main thread broke."""
        if not threading.main_thread().is_alive():
            logger.critical("Main thread is dead! Wrapping it up...")
            self.stop()

    def __check_snapshot_done(self):
        # Todo: consider concluding snapshot done when items from queue are no longer type 'snapshot'
        if self.lob.items_processed == self.__snapshot_order_count:
            logger.info(f"Snapshot processed.")
            self.__next_queue_mode()

    # Todo: condense next three methods into one
    @run_once
    def __log_first_sequence(self, sequence=None):
        """Store first sequence."""
        if sequence is None and self.sequence is not None:
            sequence = copy.deepcopy(self.sequence)
        self.first_sequence = sequence
        logger.debug(f"First sequence = {self.first_sequence}")

    @run_once
    def __log_snapshot_sequence(self, sequence=None):
        """Store snapshot sequence."""
        if sequence is None and self.sequence is not None:
            sequence = copy.deepcopy(self.sequence)
        self.snapshot_sequence = sequence
        logger.debug(f"Snapshot sequence = {self.snapshot_sequence}")

    @run_once
    def __log_websocket_sequence(self, sequence=None):
        """Store first websocket sequence."""
        if sequence is None and self.sequence is not None:
            sequence = copy.deepcopy(self.sequence)
        self.websocket_sequence = sequence
        logger.debug(f"Websocket sequence = {self.websocket_sequence}")

    def __check_missing_sequences(self, start: int, end: int):
        # Todo: abstract this away for all missing sequence scenarios
        if None not in (start, end) and start + 1 < end:
            missing_range = range(start + 1, end)
            logger.warning(f"Missing {len(missing_range)} sequences: {missing_range}")
            self.missing_sequences.extend(missing_range)

    @run_once
    def __check_post_snapshot_missing_sequences(self):
        """Assumes websocket starts feeding data before snapshot"""
        self.__check_missing_sequences(self.snapshot_sequence, self.first_sequence)

    def __check_finished(self):
        if self.queue.empty():
            self.__next_queue_mode()

    def __clear_queues_and_exit(self):
        if not self.queue.empty():
            logger.info(f"Queue is not empty. Clearing remaining {self.queue.qsize()} items and exiting thread.")
            self.queue.queue.clear()
        else:
            logger.info("Queue is empty. Exiting thread.")

    def __get_and_process_item(self, *args, **kwargs):
        """Get item from queue and call process_item.
        Raises JustContinueException if item is None, to enable use of continue in outer loop (__process_queue).
        """
        try:
            item = self.queue.get(timeout=0.01)
            if item is None:
                self.__total_queue_items_skipped += 1
                raise JustContinueException

            self.__process_item(item, *args, **kwargs)
            self.__track_perf_data()
            self.__lob_checked = False
            self.queue.task_done()

        except q.Empty as e:
            raise q.Empty

    def __process_queue(self) -> None:
        self.__timer.start()
        self.__lob_check_timer.start()
        self.__save_timer.start()
        self.__queue_stats_timer.start()
        self.__queue_empty_timer.start()
        logger.info(
            f"Queue worker starting now in {self.queue_mode} mode. Starting queue size: {self.queue.qsize()} items")

        while True:

            self.__check_main_thread()

            match self.queue_mode:

                case "snapshot":

                    # check for missing sequences between snapshot and start of websocket
                    if None not in (self.snapshot_sequence, self.first_sequence):  # redundant None check due to @run_once
                        self.__check_post_snapshot_missing_sequences()

                    try:
                        self.__get_and_process_item(output_data=False, snapshot=True)
                    except JustContinueException:
                        continue
                    else:
                        self.__output_perf_data()
                        self.__track_snapshot_load()
                    finally:
                        self.__check_snapshot_done()

                case "websocket":
                    try:
                        self.__get_and_process_item()
                    except JustContinueException:
                        continue
                    except q.Empty:
                        self.__timed_queue_empty_note()
                        self.__timed_lob_check()
                    else:
                        self.__output_perf_data()
                        self.__timed_get_queue_stats(track_delay=True)
                        self.__save(timed=True)

                case "finish":
                    try:
                        self.__get_and_process_item()
                    except JustContinueException:
                        continue
                    except q.Empty:
                        self.__output_perf_data()
                        self.__check_finished()

                case "stop":
                    self.__end_perf_data()
                    self.__save_dataframes(final=True)
                    self.__log_summary()
                    self.__clear_queues_and_exit()
                    logger.info("Queue worker has finished.")
                    break

    def __process_item(self, item: dict, output_data: bool = True, snapshot: bool = False) -> None:

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

        self.latest_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") \
            if timestamp is not None else datetime.utcnow()

        is_bid = True if side == "buy" else False

        if self.first_sequence is None and sequence is not None:
            self.__log_first_sequence(sequence)

        if self.websocket_sequence is None and sequence is not None and not snapshot:
            self.__log_websocket_sequence(sequence)

        # item validity checks ---------------------------------------------------------
        valid_sequence, valid_received, valid_open, valid_done, valid_change = True, True, True, True, True

        # sequence is invalid if it's None, out of order,
        # or if snapshot is True and the item passed wasn't a snapshot (to sync snapshot with websocket)

        if sequence is None or \
                (self.sequence is not None and sequence <= self.sequence) or \
                (snapshot and item_type != "snapshot"):
            valid_sequence = False
        else:
            self.prev_sequence = self.sequence
            self.sequence = sequence

            # check for missing sequences
            if self.prev_sequence is not None and self.sequence != self.prev_sequence + 1:
                self.__check_missing_sequences(self.prev_sequence, self.sequence)

        match item_type:
            case "snapshot":
                valid_sequence = True  # all snapshot items have the same sequence
                if self.snapshot_sequence is None:
                    self.__log_snapshot_sequence(sequence)
            case "received":
                if None in {item_type, order_id}:
                    logger.info(f"Invalid received msg: {item}")
                    valid_received = False
            case "open":
                if None in {item_type, order_id, side, remaining_size, price}:
                    logger.info(f"Invalid open msg.")
                    valid_open = False
            case "done":
                if None in {item_type, order_id}:
                    logger.info(f"Invalid done msg.")
                    valid_done = False
            case "change":
                if None in {item_type, order_id, new_size}:
                    logger.info(f"Invalid change msg.")
                    valid_change = False

        # process items ----------------------------------------------------------------
        match item_type:
            case "subscriptions":
                if self.__item_display_flags[item_type]:
                    self.display_subscription(item)

            # process received
            case "received" if valid_received and valid_sequence:
                if self.__item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
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
                if output_data:
                    self.output_data()

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
                if output_data:
                    self.output_data()

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
                if output_data:
                    self.output_data()

            # process trades
            case "match" if valid_sequence:
                self.matches.process_item(item, display_match=self.__item_display_flags[item_type])
                if self.__build_candles:
                    self.candles.process_item(item)

            case _ if not valid_sequence and snapshot:
                msg = f"Non-snapshot item provided in snapshot mode"
                msg += f" (current={self.sequence}, provided={sequence}). Skipping..."
                logger.debug(msg)

            case _ if not valid_sequence:
                msg = f"Item below provided out of sequence (current={self.sequence}, provided={sequence})"
                logger.warning(msg)
                logger.warning(f"{item}")

            case _:
                logger.critical(f"Below item's type is unhandled!")
                logger.critical(f"{item}")
                raise ValueError("Unhandled msg type")

    def output_data(self):
        if self.output_queue is not None and self.output_queue.qsize() < self.output_queue._maxsize:
            timestamp = datetime.strptime(self.lob.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%m/%d/%Y-%H:%M:%S")
            bid_levels, ask_levels = self.lob.levels
            data = {
                "timestamp": timestamp,
                "sequence": self.sequence,
                "bid_levels": bid_levels,
                "ask_levels": ask_levels
            }
            # logger.debug(f"PLACING item {self.output_item_counter}: bid_levels {bid_levels}")
            # logger.debug(f"PLACING item {self.output_item_counter}: ask_levels {ask_levels}")
            self.output_queue.put(data)

    def __track_perf_data(self):
        # if self.latest_timestamp is not None:
        delay = max(datetime.utcnow().timestamp() - self.latest_timestamp.timestamp(), 0)
        self.delays.append(delay)
        self.count += 1
        self.total_count += 1

    @run_once_per_interval("stats_queue_interval")
    def __output_perf_data(self):

        if self.stats_queue is not None:
            item = {
                "process": "orderbook_builder_thread_loop",
                "timestamp": datetime.utcnow().timestamp(),
                "elapsed": self.module_timer.elapsed(),
                "data": {
                    "total": self.total_count,
                    "count": self.count,
                    "avg delay": np.mean(self.delays) if len(self.delays) != 0 else 0,
                },
            }
            self.stats_queue.put(item)

            self.count = 0
            self.delays = deque()

    @run_once
    def __end_perf_data(self):
        """signal to performance data reader that feed is over. can only run once"""
        if self.stats_queue is not None:
            item = (
                datetime.utcnow().timestamp(),
                "orderbook_build_loop",
                "done"
            )
            self.stats_queue.put(item)

    def display_subscription(self, item: dict):
        assert len(item.get("channels")) == 1
        assert len(item["channels"][0]["product_ids"]) == 1
        self.__market = item['channels'][0]['product_ids'][0]
        line_output = f"Subscribed to {self.exchange}'s '{self.__market}' channel "
        line_output += f"for {item['channels'][0]['product_ids'][0]}"
        s_print(colored(line_output, "yellow"))

    def __log_summary(self):
        logger.info("________________________________ Summary ________________________________")
        # Todo: move log_details into here
        self.lob.log_details()
        logger.info(f"Matches processed = {self.matches.total_items:,}")
        if self.candles is not None:
            logger.info(f"Candles generated = {self.candles.total_items}")

        if len(self.__queue_stats["delta"]) != 0:
            # giving datetime.timedelta(0) as the start value makes sum work on tds
            # source: https://stackoverflow.com/questions/3617170/average-timedelta-in-list
            average_timedelta = sum(self.__queue_stats["delta"], timedelta(0)) / len(self.__queue_stats["delta"])
            logger.info(f"Average webhook-processing delay = {average_timedelta}")
            logger.info(f"LOB validity checks performed = {self.__lob_check_count}")
            logger.info(f"Total items processed from queue = {self.total_count:,}")

        if len(self.missing_sequences) != 0:
            logger.warning(f"{len(self.missing_sequences)} missing sequences.")

        # Todo: Add more

        logger.info(f"________________________________ Summary End ________________________________")


