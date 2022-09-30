from rust_orderbook import LimitOrderbook, Order, Side, Submit
import random
from datetime import datetime
import numpy as np

# {"price": "1319.26", "order_id": "76af2d47-11d1-468d-b74a-fcad11770b32",
# "remaining_size": "8.49", "type": "open", "side": "sell",
# "product_id": "ETH-USD", "time": "2022-09-27T19:31:30.869562Z", "sequence": 36673388000}

order_dict = {
    "uid": "76af2d47-11d1-468d-b74a-fcad11770b32",
    "side": "sell",
    "size": 8.49,
    "price": 1319.26,
    "timestamp": "2022-09-27T19:31:30.869562Z"
}

LOB = LimitOrderbook()

assert LOB.len == 0
assert LOB.best_ask is None
assert LOB.best_bid is None
assert LOB.node_count == 0

bid_levels = LOB.levels(Side.Bids)

print("\nbid levels")
print(bid_levels)
print("\ndestructured numpy arrays")
try:
    bid_price, bid_size, bid_depth = np.array(bid_levels).transpose()
except ValueError:
    bid_price, bid_size, bid_depth = np.empty(0), np.empty(0), np.empty(0)
print(bid_price, bid_size, bid_depth)

print("\nOrders for insertion:\n")
insert_orders = []
for i in range(0, 20):
    side = Side.Bids if random.random() >= 0.50 else Side.Asks
    price = random.uniform(0, 1000).__round__(2)
    size = random.uniform(0, 10).__round__(2)
    order = Order(str(i), side, price, size, datetime.now().strftime("%m/%d/%Y-%H:%M:%S"))
    print(order)
    insert_orders.append(order)
    LOB.process(order, Submit.Insert)

LOB.display(Side.Bids)
LOB.display(Side.Asks)

print("\nOrders for update:\n")
update_orders = random.choices(insert_orders, k=5)
for order in update_orders:
    order.size = 0.01
    print(order)
    LOB.process(order, Submit.Update)

LOB.display(Side.Bids)
LOB.display(Side.Asks)

print("\nRemaining orders:\n")
remaining_orders = list(set(insert_orders) - set(update_orders))
for order in remaining_orders:
    print(order)
    # LOB.process(order, Submit.Remove)

LOB.display(Side.Bids)
LOB.display(Side.Asks)

bid_levels = LOB.levels(Side.Bids)
ask_levels = LOB.levels(Side.Asks)

print("\nbid levels")
print(bid_levels)
print("\ndestructured numpy arrays")
bid_price, bid_size, bid_depth = np.array(list(zip(*bid_levels)))
print(bid_price, bid_size, bid_depth)

print("\nask levels")
print(ask_levels)
print("\ndestructured numpy arrays")
ask_price, ask_size, ask_depth = np.array(list(zip(*ask_levels)))
print(ask_price, ask_size, ask_depth)

notes = LOB.log_notes()
LOB.check()
print(notes)


