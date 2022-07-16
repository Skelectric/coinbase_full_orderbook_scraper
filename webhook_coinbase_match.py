"""Open authenticated websocket(s) to Coinbase's MATCH channel."""
from datetime import datetime
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
import logging
import api_coinbase
import signal
import time
import json
import os

import pandas as pd
# pd.options.mode.chained_assignment = None  # default='warn'
pd.options.display.float_format = '{:.6f}'.format

#======================================================================================
# Webhook Parameters

ENDPOINT = 'wss://ws-feed.exchange.coinbase.com'
TICKERS = ['BTC-USD', 'ETH-USD']
#TICKERS = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'AVAX-USD', 'UNI-USD', 'SNX-USD', 'AAVE-USD', 'YFI-USD', 'XMR-USD']
FREQUENCY = '1T'
#FREQUENCIES = ['1T', '5T', '15T', '1H', '4H', '1D']
SAVE_CSV = True
SAVE_HD5 = False

#======================================================================================
# Configure Logging

log_f = 'webhook_coinbase_match_log'
i = 0
while os.path.exists(f"{log_f}_{i}.log"):
    i += 1
log_f = f"{log_f}_{i}.log"

# logging.disable(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s (%(threadName)-10s) %(message)s',
                    handlers=[
                        logging.FileHandler(rf"logs\{log_f}s"),
                        logging.StreamHandler()
                    ],
                    force=True)

#======================================================================================

class GracefulKiller:
    """Help kill threads when stop called."""
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGBREAK, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    def exit_gracefully(self, *args):
        logging.info(f"Stop triggered. Ending threads.............................")
        self.kill_now = True

s_print_lock = Lock()
result_lock = Lock()

def s_print(*a, **b):
    """Thread-safe print function."""
    with s_print_lock:
        print(*a, **b)

def main():
    threads = []
    threads_keepalive = []
    ws = [None] * len(TICKERS)
    data_col = ["time", "ticker", "side", "size", "price"]
    candle_col = ["ticker", "candle", "open", "high", "low", "close", "volume"]
    # create list of dataframes so that each thread can work on its own dataframe
    data = [pd.DataFrame(columns=data_col) for i in range(len(TICKERS))]
    candles = [pd.DataFrame(columns=candle_col) for i in range(len(TICKERS))]

    def websocket_thread(ws, ticker, i, data, candles):
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

        threads_keepalive[i].start()

        # main websocket loop
        while not killer.kill_now:
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

                if msg.get("type") not in {'match', 'last_match'}:
                    s_print(msg)

                else: # parse data from msg and place into dataframes

                    _time = datetime.strptime(msg.get('time'), "%Y-%m-%dT%H:%M:%S.%fZ")
                    _id = msg.get('product_id')
                    _side = msg.get('side')
                    _size = float(msg.get('size'))
                    _price = float(msg.get('price'))
                    s_print(f"Thread {i} - {_time} --- {_side} {_size} {_id} at ${_price:,}")

                    # (re)initialize temp dataframe for easy concatenation to main dataframe
                    df = pd.DataFrame(columns=["time", "ticker", "side", "size", "price"],
                                      data=[[_time, _id, _side, _size, _price]])

                    # numpy datetime type
                    df["time"].astype("datetime64[ns]")

                    candle = df["time"].dt.floor(freq=FREQUENCY)[0] # floor time at chosen frequency
                    # # extra column to track candle
                    # df["candle"] = candle

                    if not data[i].empty:
                        # if candle != data[i]["candle"].iloc[-1]:
                        if candle != last_candle:
                            # if candle is new, concat into main candles dataframe and reset temp dataframe
                            logging.debug("-----New Candle-----")

                            df_c = pd.DataFrame(columns=list(candles[i].head()),
                                                data=[[ticker, candle, _open, high, low, close, volume]])

                            candles[i] = pd.concat([candles[i], df_c])
                            logging.debug(f"candle added: {df_c.to_string()}")

                            _open = _price
                            high = _price
                            low = _price
                            close = _price
                            volume = _size

                        # if candle not new, update high, low, close, and volume
                        else:
                            high = max(high, _price)
                            low = min(low, _price)
                            close = _price
                            volume = volume + _size

                    else:  # should run upon launch and for every new candle
                        _open = _price
                        high = _price
                        low = _price
                        close = _price
                        volume = _size

                    # logging.debug(f"candle:{candle}-open:{_open},high:{high},low:{low},close:{close},volume:{volume}")

                    # concat order results back to dataframe
                    data[i] = pd.concat([data[i], df])
                    last_candle = candle

        logging.debug(f"Thread {ticker} Loop Broke!")

        # close websocket
        try:
            if ws[i]:
                ws[i].close()
        except WebSocketConnectionClosedException:
            pass

        logging.info(f"Ended websocket thread for {ticker}.")

    def websocket_thread_keepalive(ws, i, interval=60):
        while ws[i].connected:
            time.sleep(interval)
            try:
                ws[i].ping("keepalive")
                logging.info(f"Pinged websocket for {TICKERS[i]}.")
            except WebSocketConnectionClosedException:
                pass

    user = api_coinbase.get_user()

    # handle websocket threads
    for i, ticker in enumerate(TICKERS):
        logging.info(f"Starting websocket Thread {i+1} for {ticker}.")
        thread = Thread(target=websocket_thread, args=(ws, ticker, i, data, candles))
        threads.append(thread)
        thread.start()

        # handle keepalive threads
        thread_keepalive = Thread(target=websocket_thread_keepalive, daemon=True, args=(ws, i))
        threads_keepalive.append(thread_keepalive)

    # keep main() from ending before threads are shut down
    while True:
        time.sleep(1)
        for thread in threads:
            if not thread.is_alive():
                thread.join()
                threads.remove(thread)
                logging.info(f"Active threads = {len(threads)}")
        if not threads:
            break

    data = pd.concat(data, keys=TICKERS, ignore_index=False)
    candles = pd.concat(candles, keys=TICKERS, ignore_index=False)

    order_count = len(data)
    candle_count = len(candles)

    short_ticker_txt = ''
    for t in TICKERS:
        short_ticker_txt += t[:t.find("-")]

    file_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    data_filename = f"Coinbase_{FREQUENCY}_kline_{order_count}_{short_ticker_txt}_USD_{file_timestamp}."
    candle_filename = f"Coinbase_{FREQUENCY}_kline_{candle_count}_{short_ticker_txt}_USD_{file_timestamp}"

    with pd.option_context('display.max_rows', 30, 'display.max_columns', 20):
        print(data)
        print(candles)

    if SAVE_CSV:
        data.to_csv(f"data\{data_filename}.csv")
        candles.to_csv(f"data\{candle_filename}.csv")

    if SAVE_HD5:
        data.to_csv(f"data\{data_filename}.h5")
        candles.to_csv(f"data\{candle_filename}.h5")

if __name__ == '__main__':
    start_time = datetime.now()
    killer = GracefulKiller()
    main()
    end_time = datetime.now()
    print(f"\nElapsed time = {end_time - start_time}")
