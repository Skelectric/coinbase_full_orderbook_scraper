"""Open authenticated websocket(s) to Coinbase's MATCH channel."""
# Todo: Add abstractions and refactor into classes where possible
# i.e. websocket class
# dataframe class with custom properties including filename, data type
# Todo: Refactor functions out into separate modules
# Todo: Add additional error handling (?)
from datetime import datetime
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
import logging
import api_coinbase
import signal
import time
import json
import os
import queue
from helper_tools import Timer
import math

import pandas as pd
pd.options.display.float_format = '{:.6f}'.format

# ======================================================================================
# Webhook Parameters

ENDPOINT = 'wss://ws-feed.exchange.coinbase.com'
# TICKERS = ('BTC-USD', 'ETH-USD')
TICKERS = ('BTC-USD', 'ETH-USD', 'DOGE-USD', 'SHIB-USD', 'SOL-USD',
           'AVAX-USD', 'UNI-USD', 'SNX-USD', 'CRV-USD', 'AAVE-USD', 'YFI-USD')
FREQUENCY = '1T'  # 1 min
# FREQUENCIES = ['1T', '5T', '15T', '1H', '4H', '1D']
SAVE_CSV = True
SAVE_HD5 = False #Todo: add SAVE_HD5 function
k = 360  # Save dataframe into CSV every k seconds

# ======================================================================================
# Configure Logging

log_f = 'webhook_coinbase_match_log'
_i = 0
while os.path.exists(rf"logs\{log_f}_{_i}.log"):
    _i += 1
log_f = rf"logs\{log_f}_{_i}.log"

logging.disable(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s (%(threadName)-10s) %(message)s',
                    handlers=[
                        logging.FileHandler(log_f),
                        logging.StreamHandler()
                    ],
                    force=True)

# ======================================================================================


class UnknownDataframeTypeError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

    pass


class GracefulKiller:
    """Help kill threads when stop called."""
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGBREAK, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        logging.critical(f"Stop triggered. Ending threads.............................")
        self.kill_now = True


s_print_lock = Lock()
result_lock = Lock()


def s_print(*a, **b):
    """Thread-safe print function."""
    with s_print_lock:
        print(*a, **b)


def main():
    active_threads = []
    ws = [None] * len(TICKERS)
    match_types = {'match', 'last_match'}

    data_col = ("type", "time", "ticker", "side", "size", "price")
    data = pd.DataFrame(columns=data_col)

    candle_col = ("type", "ticker", "candle", "frequency", "open", "high", "low", "close", "volume")
    candles = pd.DataFrame(columns=candle_col)

    # shortened version of tickers for output filename
    all_symbols = ','.join([t[:t.find("-")] for t in TICKERS])

    df_queue = queue.Queue()

    websockets_open = True

    def websocket_thread(ws, ticker, i, data_col, candle_col):
        nonlocal websockets_open, df_queue
        ws[i] = create_connection(ENDPOINT)
        ws[i].send(
            json.dumps(
                {
                    "type": "subscribe",
                    "Sec-WebSocket-Extensions": "permessage-deflate",
                    "user_id": user["legacy_id"],
                    "profile_id": user["id"],
                    "product_ids": [ticker],
                    "channels": ["matches"]
                }
            )
        )

        if not thread_keepalive.is_alive():
            thread_keepalive.start()

        # needed for first iteration over main websocket loop
        last_candle = None

        # main websocket loop
        while not killer.kill_now:
            feed = None
            try:
                feed = ws[i].recv()
                if feed:
                    msg = json.loads(feed)
                else:
                    msg = {}
            except (ValueError, KeyboardInterrupt, Exception) as e:
                logging.debug(e)
                logging.debug(f"{e} - data: {feed}")
                break
            else:

                # logging.debug(f"msg.get(`type`) = {msg.get('type')}")

                if msg.get("type") not in match_types:

                    if msg.get("type") == "subscriptions":
                        logging.info(msg)
                    else:
                        logging.warning(f"Unfamiliar msg type:\n{msg}")

                else: # parse data from msg and place into dataframes

                    _time = datetime.strptime(msg.get('time'), "%Y-%m-%dT%H:%M:%S.%fZ")
                    _id = msg.get('product_id')
                    _side = msg.get('side')
                    _size = float(msg.get('size'))
                    _price = float(msg.get('price'))
                    s_print(f"Thread {i} - {_time} --- {_side} {_size} {_id} at ${_price:,}")

                    # (re)initialize temp dataframe for concatenation to main dataframe
                    df = pd.DataFrame(columns=data_col,
                                      data=[[msg.get("type"), _time, _id, _side, _size, _price]])

                    # numpy datetime type
                    df["time"].astype("datetime64[ns]")

                    # place temp match dataframe into queue
                    df_queue.put(df)

                    candle = df["time"].dt.floor(freq=FREQUENCY)[0] # floor time at chosen frequency

                    # build candles
                    # should get skipped on first loop since last_candle is None until end of first loop
                    if last_candle is not None and candle != last_candle:
                        # if candle is new, concat into main candles dataframe and reset temp dataframe
                        logging.debug("-----New Candle-----")

                        df_c = pd.DataFrame(columns=candle_col,
                                            data=[["candle", ticker, last_candle, FREQUENCY,
                                                   _open, high, low, close, volume]])

                        # logging.debug(df_c)

                        # place temp candle dataframe into queue
                        df_queue.put(df_c)

                    if candle != last_candle:
                        _open = _price
                        high = _price
                        low = _price
                        close = _price
                        volume = _size

                    else: # if candle not new, update high, low, close, and volume
                        high = max(high, _price)
                        low = min(low, _price)
                        close = _price
                        volume = volume + _size

                    last_candle = candle

        logging.debug(f"Websocket thread loop for {ticker} ended.")

        # close websocket
        try:
            if ws[i]:
                ws[i].close()
        except WebSocketConnectionClosedException:
            pass

        logging.info(f"Ended websocket thread for {ticker}.")

    def websocket_thread_keepalive(interval=60):
        time.sleep(interval)
        while websockets_open:
            for j, websocket in enumerate(ws):
                try:
                    websocket.ping("keepalive")
                    logging.info(f"Pinged websocket for {TICKERS[j]}.")
                except WebSocketConnectionClosedException:
                    pass
            time.sleep(interval)

    def queue_worker():
        """Get temporary dataframes from queue and append to final order match and candle dataframes."""
        nonlocal data, candles, df_queue
        delay_int = 5
        logging.info(f"Queue worker starting in {delay_int} seconds...")
        time.sleep(delay_int)
        logging.info(f"Queue worker starting now. Starting queue size: {df_queue.qsize()} items")

        save_timer = Timer()

        # only for saving in chunks, every k seconds
        data_temp = data.copy()
        candles_temp = candles.copy()

        def process_dataframe(df):
            # process_dataframe.counter += 1
            nonlocal data, data_temp, candles, candles_temp

            # concatenate to total dataframes and maybe save
            try:
                # logging.debug(f"df_type = {get_df_type(df)}")

                # order matches
                if get_df_type(df) in match_types:
                    data = pd.concat([data, df], ignore_index=True)
                    data_temp = pd.concat([data_temp, df], ignore_index=True)

                # candles
                elif get_df_type(df) == "candle":
                    candles = pd.concat([candles, df], ignore_index=True)
                    candles_temp = pd.concat([candles_temp, df], ignore_index=True)

                else:
                    logging.debug(f"Raising UnknownDataframeTypeError")
                    raise UnknownDataframeTypeError

            except UnknownDataframeTypeError:
                logging.critical("Unknown exception. Dataframe processing failed.")
                pass

        def save_temp_dataframes(final: bool = False):
            """Saves temporary dataframes to filesystem and clears their contents.
            If final=True, rename files to include dataframe element count"""
            nonlocal data_temp, candles_temp, match_df_filename, candle_df_filename

            if match_df_filename is None and not data_temp.empty:
                match_df_filename = derive_df_filename(data_temp)
                if final:
                    prev_match_df_filename = match_df_filename
                    match_df_filename = derive_df_filename(data, final=True)
                    if os.path.exists(prev_match_df_filename):
                        os.rename(prev_match_df_filename, match_df_filename)

            if candle_df_filename is None and not candles_temp.empty:
                candle_df_filename = derive_df_filename(candles_temp)
                if final:
                    prev_candle_df_filename = candle_df_filename
                    candle_df_filename = derive_df_filename(candles, final=True)
                    if os.path.exists(prev_candle_df_filename):
                        os.rename(prev_candle_df_filename, candle_df_filename)

            if not data_temp.empty:
                save_dataframe(data_temp, match_df_filename)
                logging.debug("Match data saved.")
                logging.info(f"Final match file saved to /data:\n----{match_df_filename}.csv")

            if not candles_temp.empty:
                save_dataframe(candles_temp, candle_df_filename)
                logging.debug("Candle data saved.")
                logging.info(f"Final candle file saved to /data:\n----{candle_df_filename}.csv")

            # clear temp dataframes
            data_temp = data_temp.iloc[0:0]
            candles_temp = candles_temp.iloc[0:0]

        # process_dataframe.counter = 0

        queue_timer = Timer()
        queue_timer_limit = 10
        queue_sizes = []
        queue_timer.start()

        save_timer.start()

        match_df_filename = None
        candle_df_filename = None

        # get item -> process & save -> done
        while True:

            queue_sizes.insert(0, df_queue.qsize())
            queue_sizes = queue_sizes[:1000]  # limit to last 1000 queue sizes measured

            try:
                item = df_queue.get(timeout=0.01)
                # logging.debug(f"item =\n{item}")

                if item is None:
                    logging.debug(f"item is None: {item is None}")
                    continue

                try:
                    process_dataframe(item)
                    # logging.debug(f"Item processed. Queue size: {df_queue.qsize()} items")

                finally:
                    df_queue.task_done()

                if (SAVE_CSV or SAVE_HD5) and save_timer.check() >= k:
                    save_timer.reset()
                    save_temp_dataframes()

            except queue.Empty:
                # logging.debug("Queue is Empty.")
                pass

            # Todo: make queue_sizes a numpy array and figure out how to work with fixed array size limitation
            if queue_timer.check() >= queue_timer_limit:
                avg_queue_size = sum(queue_sizes) / len(queue_sizes)
                logging.info(f"Average queue size: {avg_queue_size}")
                queue_timer.reset()

            if not websockets_open and (len(data_temp) != 0 or len(candles_temp) != 0):
                save_temp_dataframes(final=True)
                break

    def get_df_type(df):
        """Assumes cells are populated and first column is type"""
        return df.iloc[0][0]

    def derive_df_filename(df, final=False):

        body_dict = {"match": "{df_size}_order_matches",
                     "last_match": "{df_size}_order_matches",
                     "candle": "{FREQUENCY}_OHLC_{df_size}_candles"}

        def fstr(template):
            """Evaluate string as an f-string."""
            nonlocal df_size
            global FREQUENCY
            return eval(f"f'{template}'")

        try:
            df_type = get_df_type(df)
        except IndexError:
            df_type = "null"

        body = body_dict[df_type]

        df_size = 'X'
        if final: # Todo: add logic for inserting dataframe length into filename at end
            df_size = len(df)

        filename = f"Coinbase_{fstr(body)}_{all_symbols}_USD_{timestamp}_CHUNKED"

        return filename

    def save_dataframe_full(df: pd.DataFrame, filename: str):
        """Save dataframe in full"""
        # global SAVE_CSV, SAVE_HD5
        if SAVE_CSV:
            df.to_csv(rf"data\{filename}.csv", index=False)
        if SAVE_HD5:
            df.to_hdf(rf"data\{filename}.csv", index=False)

    def save_dataframe(df: pd.DataFrame, filename: str):
        """Save dataframe chunk using append"""
        os.makedirs('data', exist_ok=True)
        if not os.path.exists(rf"data\{filename}.csv"):
            header = True
        else:
            header = False
        df.to_csv(rf"data\{filename}.csv", index=False, mode='a', header=True)
        logging.info(f"Saved df into {filename}.")

    # on Coinbase, enables authenticated match channel subscription
    user = api_coinbase.get_user()

    # handle websocket threads
    for i, ticker in enumerate(TICKERS):
        logging.info(f"Starting websocket Thread {i} for {ticker}.")
        thread = Thread(target=websocket_thread, args=(ws, ticker, i, data_col, candle_col))
        active_threads.append(thread)
        thread.start()

    # define keepalive thread
    thread_keepalive = Thread(target=websocket_thread_keepalive, daemon=True)

    worker = Thread(target=queue_worker)
    worker.start()

    # keep main() from ending before threads are shut down
    while True:
        time.sleep(1)
        for thread in active_threads:
            if not thread.is_alive():
                # thread.join()
                active_threads.remove(thread)
                logging.info(f"Active threads = {len(active_threads)}")
        if not active_threads:
            websockets_open = False
            break

    worker.join()

    with pd.option_context('display.max_rows', 30, 'display.max_columns', 20, 'display.width', 1000):
        print(f"\n\nMatch data preview:\n{data}\n\n")
        print(f"Candle data preview:\n{candles}\n\n")

    # handle output filenames and save if options are selected and dataframes are not empty
    order_count = len(data)
    candle_count = len(candles)
    file_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    data_filename = f"Coinbase_{order_count}_order_matches_{all_symbols}_USD_{file_timestamp}."
    candle_filename = f"Coinbase_{FREQUENCY}_OHLC_{candle_count}_candles_{all_symbols}_USD_{file_timestamp}"

    if order_count != 0:
        save_dataframe_full(data, data_filename)
    if candle_count != 0:
        save_dataframe_full(candles, candle_filename)


if __name__ == '__main__':
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    module_timer = Timer()
    module_timer.start()
    killer = GracefulKiller()
    main()
    module_timer.stop()
