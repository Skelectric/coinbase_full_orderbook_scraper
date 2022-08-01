"""Adapted from https://github.com/kmanley/orderbook/blob/master/orderbook.py"""

from collections import deque


class Order:
    def __init__(self, side, size, price, trader, order_id):
        """
        Class representing a single price level of aggregated orders.
        :param side: 0=buy, 1=sell
        :param size: order quantity
        :param price: price in ticks
        :param trader: owner of order
        :param order_id: order ID
        """
        self.side = side
        self.size = size
        self.price = price
        self.trader = trader
        self.order_id = order_id

class Orderbook(object):
    def __init__(self, market, min_price, max_price, start_order_id=0, cb=None):
        """
        Class representing an orderbook
        :param market: market being represented by orderbook
        :param min_price: minimum price in ticks
        :param max_price: maximum price in ticks
        :param start_order_id: first number to use for next order
        :param cb: trade execution callback
        """
        self.market = market
        self.order_id = start_order_id
        self.min_price = min_price
        self.max_price = max_price
        self.cb = cb or self.execute
        self.price_points = [deque() for i in range(self.max_price + 0.01)]
        self.bid_max = 0
        self.ask_min = max_price + 1
        self.orders = {}  # order id -> order

    def execute(self, trader_buy, trader_sell, price, size):
        """
        Execution callback
        :param trader_buy: trader on the buy side
        :param trader_sell: trader on the sell side
        :param price: trade price
        :param size: trade size
        """
        print(f"Execute: {trader_buy} BUY {trader_sell} SELL {size} {self.market} @ {price}")

    def limit_order(self, ):