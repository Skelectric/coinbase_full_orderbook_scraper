import json
import gzip
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
from plotting.performance import PerfPlotQueueItem

import numpy as np


class JustContinueException(Exception):
    pass


class OrderbookSnapshotHandler:
    """class that loads dict containing orderbook snapshot into the OrderbookBuilder's input queue"""
    def __init__(self, queue: q.Queue,
                 depth: int = None,
                 orderbook_snapshot: dict = None,
                 **kwargs):
        self.order_count = 0

        if orderbook_snapshot is None:
            snapshot_filepath = kwargs.get("snapshot_filepath", None)
            if snapshot_filepath is None:
                raise Exception("No orderbook snapshot or orderbook snapshot file provided!")
            else:
                orderbook_snapshot = self.load_orderbook_snapshot(snapshot_filepath)

        if kwargs.get("save", False):
            logger.debug(f"Saving orderbook snapshot...")
            self.save_orderbook_snapshot(orderbook_snapshot, **kwargs)

        self.load_snapshot_to_queue(queue, orderbook_snapshot, depth)

    @staticmethod
    def load_orderbook_snapshot(snapshot_filepath: Path):
        logger.debug(f"Loading snapshot from {snapshot_filepath.name}")
        with gzip.open(snapshot_filepath, 'rt', encoding='UTF-8') as f:
            snapshot = json.loads(f.read())
        return snapshot

    @staticmethod
    def save_orderbook_snapshot(*args, **kwargs):
        """Save orderbook object as a gzip file."""
        orderbook_snapshot = args[0]
        market = kwargs.get("market", "n/a")
        folder = kwargs.get("save_folder", "data")
        exchange = kwargs.get("exchange", "exchange")
        Path(folder).mkdir(parents=True, exist_ok=True)
        sequence = orderbook_snapshot["sequence"]
        filename_timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        snapshot_filename = f"{exchange}_orderbook_snapshot_{market}_{sequence}_{filename_timestamp}.json.gz"
        snapshot_filepath = Path.cwd() / folder / snapshot_filename
        with gzip.open(snapshot_filepath, 'wt', encoding='UTF-8') as f:
            f.write(json.dumps(orderbook_snapshot))
        logger.debug(f"Snapshot saved to {snapshot_filename}.")

    def load_snapshot_to_queue(self, _queue, orderbook_snapshot, depth) -> None:
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
    def __init__(self, queue: q.Queue, **kwargs):
        # queue worker options
        self.__snapshot_order_count = kwargs.get("snapshot_order_count", 0)  # int
        self.__save_matches = kwargs.get("save_matches", False)  # bool
        self.__save_candles = kwargs.get("save_candles", False)  # bool
        self.__output_folder = kwargs.get("output_folder", "data")  # str
        self.__keep_matches_in_memory = kwargs.get("keep_matches_in_memory", True)
        self.__keep_candles_in_memory = kwargs.get("keep_candles_in_memory", True)

        # misc
        self.module_timer = kwargs.get("module_timer", Timer())  # Timer
        self.__exchange = kwargs.get("exchange", None)  # str
        self.__market = kwargs.get("market", None)  # str

        self.__item_display_flags = {
            "subscriptions": False,
            "received": False,
            "open": False,
            "done": False,
            "match": False,
            "change": False,
            "snapshot": False
        }

        item_display_flags = kwargs.get("item_display_flags", None)  # dict
        if isinstance(item_display_flags, dict):
            self.__item_display_flags.update(item_display_flags)

        if self.module_timer.get_start_time() is None:
            self.module_timer.start()
        module_timestamp = self.module_timer.get_start_time(_format="datetime").strftime("%Y%m%d-%H%M%S")
        # build match dataframe
        self.__build_matches = kwargs.get("build_matches", True)  # bool
        self.matches = MatchDataFrame(
            exchange=self.__exchange,
            market=self.__market,
            timestamp=module_timestamp,
            output_folder=self.__output_folder
        )

        self.__build_candles = kwargs.get("build_candles", False)  # bool
        # build candles dataframe
        self.candles = None
        if self.__build_candles:
            self.candles = CandleDataFrame(
                exchange=self.__exchange,
                market=self.__market,
                frequency="1T",
                timestamp=module_timestamp,
                output_folder=self.__output_folder
            )

        self._save_interval = kwargs.get("save_interval", 360)  # float (seconds)
        self.__save_timer = Timer()

        # data structures
        self.queue = queue
        self.lob = None
        if kwargs.get("build_orderbook", True):
            self.lob = LimitOrderBook()

        # queue mode tracking and debugging
        self.__first_sequence = None
        self.__snapshot_sequence = None
        self.__first_websocket_sequence = None
        self.__missing_sequences = []
        self.__prev_sequence = None
        self.__sequence = None
        self.latest_timestamp = None

        self.output_queue = kwargs.get("output_queue", None)  # mp.queue
        if self.output_queue is not None:
            assert type(self.output_queue) == type(mp.Queue()), "passed output queue is not a multiprocessing queue!"
        if self.output_queue is not None:
            # q.Queue has maxsize, whereas mp.Queue has _maxsize
            assert hasattr(self.output_queue, '_maxsize')

        # queue stats
        self.__lob_builder_timer = Timer()
        self.__queue_empty_timer = Timer()
        self._queue_empty_interval = 60
        self.__queue_stats_timer = Timer()
        self._queue_stats_interval = 60  # seconds
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
        self._snapshot_stats_interval = 5
        self.__queue_modes = ("snapshot", "backfill", "websocket", "finish", "stop")
        self.__queue_modes_cycle = cycle(self.__queue_modes)
        self.queue_mode = None
        self.__next_queue_mode()

        # skip snapshot and backfill modes if 0 snapshot items loaded, or if not building orderbook
        # else, initialize backfill queue
        if self.__snapshot_order_count == 0 or self.lob is None:
            logger.debug("No snapshot items.")
            self.__skip_to_next_queue_mode("websocket")
        elif self.lob is None:
            logger.debug("Build orderbook set to false.")
            self.__skip_to_next_queue_mode("websocket")
        else:
            self.__backfill_queue = q.Queue()
            self.__backfill_item_count = 0
            self.__backfill_items_processed = 0

        # main processing thread
        self.thread = Thread(target=self.__process_queue)

        # use for running local copies of feeds
        self.__load_feed = False
        load_feed_filepath = kwargs.get("load_feed_filepath", None)  # Path
        if load_feed_filepath is not None:
            self.__load_feed = True
            msg = "Path passed to load_feed_filepath. "
            msg += f"Orderbook builder will queue up items from {load_feed_filepath.name}. Ensure websocket is OFF."
            logger.info(msg)
            self.__local_feed = gzip.open(load_feed_filepath, 'rt', encoding='UTF-8')
            logger.debug(f"Opened {load_feed_filepath} with gzip.")

        # performance monitoring
        self.stats_queue = kwargs.get("stats_queue", None)
        if self.stats_queue is not None:
            assert type(self.stats_queue) == type(mp.Queue()), "passed stats queue is not a multiprocessing queue!"
            self.ob_builder_perf = PerfPlotQueueItem("orderbook_builder_thread", module_timer=self.module_timer)

            self.order_insert_perf = PerfPlotQueueItem(
                "order_insert", module_timer=self.module_timer, count=False, latency=False, delta=True)
            self.order_remove_perf = PerfPlotQueueItem(
                "order_remove", module_timer=self.module_timer, count=False, latency=False, delta=True)

            self.stats_queue_interval = kwargs.get("stats_queue_interval", 1.0)  # float

    def __load_queue_with_next(self):
        assert self.__load_feed is True
        assert not self.__local_feed.closed
        line = self.__local_feed.readline()
        if line != '' and line is not None:
            # logger.debug(f"line = '{line}'")
            try:
                item = json.loads(line)
                # logger.debug(f"putting item in queue = {item}")
            except json.decoder.JSONDecodeError as e:
                logger.critical(f"JSONDecodeError from line '{line}'")
            else:
                self.queue.put(item)

    def __close_feed_file(self):
        if self.__load_feed and not self.__local_feed.closed:
            self.__local_feed.close()

    def __next_queue_mode(self):
        self.queue_mode = next(self.__queue_modes_cycle)
        logger.debug(f"Orderbook builder queue processing mode set to '{self.queue_mode}'")

    def __skip_to_next_queue_mode(self, mode: str):
        assert mode in self.__queue_modes
        mode_index = self.__queue_modes.index(mode)
        current_index = self.__queue_modes.index(self.queue_mode)
        if current_index < mode_index:
            while self.queue_mode != mode:
                self.__next_queue_mode()

    def finish(self):
        self.__skip_to_next_queue_mode("finish")
        logger.info(f"Wrapping up the queue... Remaining items: {self.queue.qsize()}")

    def stop(self):
        self.__skip_to_next_queue_mode("stop")
        logger.debug(f"Stop called. Skipping any remaining items in queue.")

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

    def __check_backfill_done(self):
        if self.__backfill_queue.empty():
            msg = f"Backfill processed. {self.__backfill_items_processed} out of "
            msg += f"{self.__backfill_item_count} had a valid sequence."
            logger.info(msg)
            self.__next_queue_mode()

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
        Raises JustContinueException if item is None, to enable use of continue in outer function (__process_queue).
        """

        backfill = kwargs.get("backfill", False)

        try:

            if backfill:
                try:
                    item = self.__backfill_queue.get(block=False)
                except q.Empty:
                    raise q.Empty

            else:
                # use timeout instead of non-blocking because block=False
                # results in frequent wasted event loop and high latency
                item = self.queue.get(timeout=1)

            if item is None:
                self.__total_queue_items_skipped += 1
                raise JustContinueException

            try:
                self.__process_item(item, *args, **kwargs)
            except AttributeError as e:
                logger.warning(f"Attribute Error: {e} caused by {item}")
            else:
                self.ob_builder_perf.track(timestamp=self.latest_timestamp.timestamp())
                self.__lob_checked = False

        except q.Empty as e:
            raise q.Empty

    def __process_queue(self) -> None:
        self.__lob_builder_timer.start()
        self.__lob_check_timer.start()
        self.__save_timer.start()
        self.__queue_stats_timer.start()
        self.__queue_empty_timer.start()
        logger.info(
            f"Orderbook builder starting now in {self.queue_mode} mode. Starting queue size: {self.queue.qsize()} items")

        while True:

            if self.__load_feed and self.queue_mode not in {'finish', 'stop'}:
                self.__load_queue_with_next()

            self.__check_main_thread()

            match self.queue_mode:

                case "snapshot":

                    # check for missing sequences between snapshot and start of websocket
                    if None not in (self.__snapshot_sequence, self.__first_sequence):  # redundant None check due to @run_once
                        self.__log_post_snapshot_missing_sequences()

                    try:
                        self.__get_and_process_item(output_data=False, snapshot=True)
                    except JustContinueException:
                        continue
                    else:
                        self.__output_perf_data()
                        self.__track_snapshot_load()
                    finally:
                        self.__check_snapshot_done()

                case "backfill":

                    self.__log_processing_backfill()

                    try:
                        self.__get_and_process_item(backfill=True)
                    except JustContinueException:
                        continue
                    except q.Empty:
                        self.__check_backfill_done()
                    else:
                        self.__output_perf_data()

                case "websocket":
                    try:
                        self.__get_and_process_item()
                    except JustContinueException:
                        continue
                    except q.Empty:
                        self.__timed_queue_empty_note()
                        self.__timed_lob_check()

                        if self.__load_feed:
                            self.__next_queue_mode()

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
                    self.__end_output_data()
                    self.__end_perf_data()
                    self.__save(final=True)
                    self.__log_summary()
                    self.__clear_queues_and_exit()
                    self.__close_feed_file()
                    logger.info("Orderbook builder has finished.")
                    break

    def __process_item(self, item: dict, output_data: bool = True, *args, **kwargs) -> None:

        # logger.debug(f"processing item = {item}")

        snapshot = kwargs.get("snapshot", False)
        backfill = kwargs.get("backfill", False)

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

        if self.__first_sequence is None and sequence is not None:
            self.__log_first_sequence(sequence)

        if self.__first_websocket_sequence is None and sequence is not None and not snapshot and not backfill:
            self.__log_first_websocket_sequence(sequence)

        # item validity checks ---------------------------------------------------------
        valid_sequence, valid_received, valid_open, valid_done, valid_change = True, True, True, True, True

        # sequence is invalid if it's None, out of order,
        # or if snapshot is True and the item passed wasn't a snapshot (to sync snapshot with websocket)

        if sequence is None or \
                (self.__sequence is not None and sequence <= self.__sequence) or \
                (snapshot and item_type != "snapshot"):
            valid_sequence = False
        else:
            self.__prev_sequence = self.__sequence
            self.__sequence = sequence

            # check for missing sequences
            if self.__prev_sequence is not None and self.__sequence != self.__prev_sequence + 1:
                self.__log_missing_sequences(self.__prev_sequence, self.__sequence)

        match item_type:
            case "snapshot":
                valid_sequence = True  # all snapshot items have the same sequence
                if self.__snapshot_sequence is None:
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

                if self.__item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("OPEN", 'yellow'), end=' ')
                    s_print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                if self.lob is not None:

                    order = Order(
                        uid=order_id,
                        is_bid=is_bid,
                        size=float(remaining_size),
                        price=float(price),
                        timestamp=timestamp
                    )

                    self.order_insert_perf.timedelta()  # reset timer
                    self.lob.process(order, action="add")
                    self.order_insert_perf.timedelta(log=True)  # log elapsed

                    if output_data:
                        self.output_data()

            # process order cancels
            case "done" if valid_done and valid_sequence:

                if self.__item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("CLOSE", 'magenta'), end=' ')
                    s_print(f"Order -- {side} {remaining_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                if self.lob is not None:

                    order = Order(
                        uid=order_id,
                        is_bid=is_bid,
                        size=None,
                        price=None,
                        timestamp=timestamp,
                    )

                    self.order_remove_perf.timedelta()  # reset timer
                    self.lob.process(order, action="remove")
                    self.order_remove_perf.timedelta(log=True)  # log elapsed

                    if output_data:
                        self.output_data()

            # process order changes
            case "change" if valid_change and valid_sequence:

                if self.__item_display_flags[item_type]:
                    s_print("------------------------------------------------------------------------")
                    s_print(colored("CHANGE", 'cyan'), end=' ')
                    s_print(f"Order -- {side} {new_size} units @ {price}", end=' ')
                    s_print(f"-- order_id = {order_id} -- timestamp: {timestamp}")

                if self.lob is not None:

                    order = Order(
                        uid=order_id,
                        is_bid=is_bid,
                        size=float(new_size),
                        price=None,
                        timestamp=timestamp,
                    )

                    self.lob.process(order, action="change")

                    if output_data:
                        self.output_data()

            # process trades
            case "match" if valid_sequence:
                self.matches.process_item(
                    item,
                    display_match=self.__item_display_flags[item_type],
                    store_in_df=self.__build_matches
                )
                if self.__build_candles:
                    self.candles.process_item(item)

            # place items into backfill queue while snapshot is processing
            # valid_sequence is false when queue mode is snapshot and item_type is not snapshot
            case _ if not valid_sequence and snapshot:
                self.__log_backfilling(sequence)
                self.__backfill_queue.put(item)

            case _ if not valid_sequence:
                self.__log_invalid_sequence(sequence, item)

            case _:
                logger.critical(f"Below item's type is unhandled!")
                logger.critical(f"{item}")
                raise ValueError("Unhandled msg type")

        if backfill and valid_sequence:
            self.__backfill_items_processed += 1

    def output_data(self):
        # Using try-except as in __end_output_data() results in latency climb
        if self.output_queue is not None and self.output_queue.qsize() < self.output_queue._maxsize:
            timestamp = datetime.strptime(self.lob.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%m/%d/%Y-%H:%M:%S")
            bid_levels, ask_levels = self.lob.levels
            data = {
                "timestamp": timestamp,
                "sequence": self.__sequence,
                "bid_levels": bid_levels,
                "ask_levels": ask_levels
            }
            # logger.debug(f"PLACING item {self.output_item_counter}: bid_levels {bid_levels}")
            # logger.debug(f"PLACING item {self.output_item_counter}: ask_levels {ask_levels}")
            self.output_queue.put(data, block=False)

    def __end_output_data(self):
        if self.output_queue is not None:
            try:
                self.output_queue.put(None)
            except q.Full:
                pass
            else:
                logger.debug(f"Orderbook builder sent 'None' to output queue.")

    @run_once_per_interval("stats_queue_interval")
    def __output_perf_data(self):
        if self.stats_queue is not None:
            self.ob_builder_perf.send_to_queue(self.stats_queue)
            self.order_insert_perf.send_to_queue(self.stats_queue)
            self.order_remove_perf.send_to_queue(self.stats_queue)

    @run_once
    def __end_perf_data(self):
        if self.stats_queue is not None:
            self.ob_builder_perf.signal_end_item()
            self.order_insert_perf.signal_end_item()
            self.order_remove_perf.signal_end_item()

            self.ob_builder_perf.send_to_queue(self.stats_queue)
            self.order_insert_perf.send_to_queue(self.stats_queue)
            self.order_remove_perf.send_to_queue(self.stats_queue)
            logger.debug(f"Orderbook builder sent 'None' to stats queue.")

    def display_subscription(self, item: dict):
        market = item['channels'][0]['product_ids'][0]
        line_output = f"Subscribed to {self.__exchange}'s '{market}' channel "
        line_output += f"for {item['channels'][0]['product_ids'][0]}"
        s_print(colored(line_output, "yellow"))

    def __log_summary(self):
        logger.info("________________________________ Summary ________________________________")
        if self.lob is not None:
            self.lob.log_details()
        logger.info(f"Matches processed = {self.matches.total_items:,}")
        if self.candles is not None:
            logger.info(f"Candles generated = {self.candles.total_items}")

        if len(self.__queue_stats["delta"]) != 0:
            # giving datetime.timedelta(0) as the start value makes sum work on tds
            # source: https://stackoverflow.com/questions/3617170/average-timedelta-in-list
            average_timedelta = sum(self.__queue_stats["delta"], timedelta(0)) / len(self.__queue_stats["delta"])
            logger.info(f"Average latency = {average_timedelta}")
            logger.info(f"LOB validity checks performed = {self.__lob_check_count}")
            logger.info(f"Total items processed from queue = {self.ob_builder_perf.total:,}")

        if len(self.__missing_sequences) != 0:
            logger.warning(f"{len(self.__missing_sequences)} missing sequences.")

        # Todo: Add more

        logger.info(f"________________________________ Summary End ________________________________")

    # todo: condense next 3 methods into one
    @run_once
    def __log_first_sequence(self, sequence=None):
        """Store first sequence."""
        if sequence is None and self.__sequence is not None:
            sequence = copy.deepcopy(self.__sequence)
        self.__first_sequence = sequence
        logger.debug(f"First sequence = {self.__first_sequence}")

    @run_once
    def __log_snapshot_sequence(self, sequence=None):
        """Store snapshot sequence."""
        if sequence is None and self.__sequence is not None:
            sequence = copy.deepcopy(self.__sequence)
        self.__snapshot_sequence = sequence
        logger.debug(f"Snapshot sequence = {self.__snapshot_sequence}")

    @run_once
    def __log_first_websocket_sequence(self, sequence=None):
        """Store first websocket sequence."""
        if sequence is None and self.__sequence is not None:
            sequence = copy.deepcopy(self.__sequence)
        self.__first_websocket_sequence = sequence
        logger.debug(f"Websocket sequence = {self.__first_websocket_sequence}")

    def __log_missing_sequences(self, start: int, end: int) -> int:
        """Logs missing sequences into log and list object, and returns count of marginal missing sequences"""
        missing_range = range(0, 0)
        if None not in (start, end):
            missing_range = range(start + 1, end)
            # logger.debug(f"missing_range = {missing_range}, length = {len(missing_range)}")
            if len(missing_range) != 0:
                logger.warning(f"Missing {len(missing_range)} sequences: {missing_range}")
                self.__missing_sequences.extend(missing_range)
        return len(missing_range)

    @run_once
    def __log_post_snapshot_missing_sequences(self):
        """Assumes websocket starts feeding data before snapshot"""
        # logger.debug(f"calling __log_missing_sequences on start={self.snapshot_sequence}, end={self.first_sequence}")
        count = self.__log_missing_sequences(self.__snapshot_sequence, self.__first_sequence)
        if count > 0:
            msg = f"Snapshot sequence is too early! Increase delay between opening websocket and requesting snapshot."
            logger.warning(msg)
        else:
            msg = f"Snapshot sequence is after first sequence from websocket -> OK."
            logger.debug(msg)

    @run_once
    def __log_processing_backfill(self):
        if self.__backfill_queue.qsize() != 0:
            logger.info(f"Processing {self.__backfill_queue.qsize()} items in backfill queue...")
            self.__backfill_item_count = self.__backfill_queue.qsize()

    @run_once_per_interval(0.1)
    def __log_backfilling(self, sequence):
        msg = f"Non-snapshot item (sequence {sequence}) provided in snapshot mode. Placing into backfill queue..."
        logger.debug(msg)

    def __log_invalid_sequence(self, sequence, item):
        if self.queue_mode != 'backfill':
            msg = f"Item below provided out of sequence (current={self.__sequence}, provided={sequence})"
            logger.warning(msg)
            logger.warning(f"{item}")

    def __save_dataframes(self, final: bool = False) -> None:
        if not (self.__save_matches or self.__save_candles):
            return
        logger.info(f"Saving dataframes. Time elapsed: {self.module_timer.elapsed(_format='hms')}")

        if self.__save_matches and isinstance(self.matches, MatchDataFrame) and not self.matches.is_empty:
            self.matches.save_chunk(csv=self.__save_matches, update_filename_flag=final)
            if not self.__keep_matches_in_memory:
                self.matches.clear()

        if self.__save_candles and isinstance(self.candles, CandleDataFrame) and not self.candles.is_empty:
            self.candles.save_chunk(csv=self.__save_matches, update_filename_flag=final)
            if not self.__keep_candles_in_memory:
                self.candles.clear()

    @run_once_per_interval("_save_interval")
    def __timed_save_dataframes(self, *args, **kwargs) -> None:
        self.__save_dataframes(*args, **kwargs)

    def __save(self, timed: bool = False, *args, **kwargs) -> None:
        if timed:
            self.__timed_save_dataframes(*args, **kwargs)
        else:
            self.__save_dataframes(*args, **kwargs)

    @run_once_per_interval("_lob_check_interval")
    def __timed_lob_check(self) -> None:
        if not self.__lob_checked and self.lob is not None:
            logger.info(f"Checking orderbook validity...", end='')
            self.lob.check()
            self.__lob_checked = True
            logger.info(f"Orderbook checked.")
            self.__lob_check_count += 1

    @run_once_per_interval("_queue_empty_interval")
    def __timed_queue_empty_note(self) -> None:
        logger.info(f"Queue empty...")

    def __get_queue_stats(
            self, track_average: bool = False, track_delay: bool = False,
            snapshot_mode: bool = False) -> None:

        msg = ''

        if track_average:
            self.__queue_stats["time"].append(self.__lob_builder_timer.elapsed())
            self.__queue_stats["queue_sizes"].append(self.queue.qsize())
            avg_qsize = sum(self.__queue_stats["queue_sizes"]) / len(self.__queue_stats["queue_sizes"])
            logger.info(f"Average queue size over {self.__lob_builder_timer.elapsed(hms_format=True)}: {avg_qsize:.2f}")
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
            dones_per_sec = self.ob_builder_perf.marginal // delta.total_seconds()
            logger.info(f"Items processed per second = {dones_per_sec:,}")
            self.__last_timestamp = datetime.utcnow()

    @run_once_per_interval("_queue_stats_interval")
    def __timed_get_queue_stats(self, *args, **kwargs):
        self.__get_queue_stats(*args, **kwargs)

    @run_once_per_interval("_snapshot_stats_interval")
    def __track_snapshot_load(self):
        self.__get_queue_stats(snapshot_mode=True)
