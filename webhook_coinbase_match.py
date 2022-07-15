"""Open authenticated websockets to Coinbase for a feed of auction matches."""
from contextlib import contextmanager
from datetime import datetime
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
import logging
import api_coinbase
import signal
import sys
import time
import json
import pandas as pd

ENDPOINT = 'wss://ws-feed.exchange.coinbase.com'
TICKERS = ['BTC-USD', 'ETH-USD', 'UNI-USD', 'SNX-USD', 'AAVE-USD']

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s (%(threadName)-10s) %(message)s',
                    )

class GracefulKiller:
    """Help kill threads when stop called."""
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGBREAK, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    def exit_gracefully(self, *args):
        logging.debug(f"Stop triggered. Ending threads.............................")
        self.kill_now = True

s_print_lock = Lock()
result_lock = Lock()

def s_print(*a, **b):
    """Thread-safe print function."""
    with s_print_lock:
        print(*a, **b)

# @contextmanager
# def non_blocking_lock(lock=Lock()):
#     """Lock that doesn't block."""
#     locked = lock.acquire(blocking=False)
#     try:
#         yield locked
#     finally:
#         if locked:
#             lock.release()

def main():
    threads = []
    data = [None] * len(TICKERS)

    def websocket_thread(ticker, data, i):
        ws = create_connection(ENDPOINT)
        data[i] = {}
        data[i]['ticker'] = ticker
        data[i]['order_count'] = 0
        logging.debug(f"Preview of data:\n{data}")

        ws.send(
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

        order_count = 1
        while not killer.kill_now:
            try:
                feed = ws.recv()
                if feed:
                    msg = json.loads(feed)
                else:
                    msg = {}
            except (ValueError, KeyboardInterrupt, Exception) as e:
                logging.debug(e)
                logging.debug(f"{e} - data: {feed}")
                break
            else:
                # print readable messages
                if msg.get("type") not in {'match', 'last_match'}:
                    s_print(order_count, msg)
                else:
                    _time = msg.get('time')[:msg.get('time').find('.')]
                    _time = datetime.fromisoformat(_time).strftime("%m/%d/%Y, %H:%M:%S")
                    _id = msg.get('product_id')
                    _side = msg.get('side')
                    _size = msg.get('size')
                    _price = float(msg.get('price'))
                    s_print(f"Thread {i+1} - Order {order_count:,} {_time} --- {_side} {_size} {_id} at ${_price:,}")
                    order_count += 1
                    # Todo: change data into a dataframe
                    # with non_blocking_lock(result_lock) as locked:
                    #     if locked:

        data[i]['order_count'] = order_count
        logging.debug(f"Thread {i} order count = {order_count}")
        logging.debug(f"Preview of data:\n{data}")

        # close websocket
        try:
            if ws:
                ws.send(
                    json.dumps(
                        {
                            "type": "unsubscribe",
                            "Sec-WebSocket-Extensions": "permessage-deflate",
                            "user_id": user["legacy_id"],
                            "profile_id": user["id"],
                            "product_ids": [ticker],
                            "channels": ["matches"]
                        }
                    )
                )
                ws.close()
        except WebSocketConnectionClosedException:
            pass

        logging.debug(f"Ended websocket thread for ticker {ticker}.")

    user = api_coinbase.get_user()

    for i, ticker in enumerate(TICKERS):
        logging.debug(f"Starting websocket Thread {i+1} for ticker {ticker}.")
        thread = Thread(target=websocket_thread, args=(ticker, data, i))
        threads.append(thread)
        thread.start()

    while True:  # required to allow for thread killing
        time.sleep(1)
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        if not threads:
            break

if __name__ == '__main__':
    start_time = datetime.now()
    killer = GracefulKiller()
    main()
    end_time = datetime.now()
    print(f"Elapsed time = {end_time - start_time}")
