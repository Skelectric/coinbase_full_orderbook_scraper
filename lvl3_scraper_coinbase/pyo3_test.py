from rust_orderbook import LimitOrderbook, Order, Side, Submit
import random
from datetime import datetime

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

print("\nOrders for insertion:\n")
insert_orders = []
for i in range(0, 20):
    side = Side.Bids if random.random() >= 0.50 else Side.Asks
    price = random.uniform(0, 1000).__round__(2)
    size = random.uniform(0, 1000).__round__(2)
    order = Order(str(i), side, price, size, datetime.now().strftime("%m/%d/%Y-%H:%M:%S"))
    print(order)
    insert_orders.append(order)
    LOB.process(order, Submit.Insert)

LOB.display_trees(Side.Bids)
LOB.display_trees(Side.Asks)

print("\nOrders for update:\n")
update_orders = random.choices(insert_orders, k=8)
for order in update_orders:
    order.size = 1000
    print(order)
    LOB.process(order, Submit.Update)

LOB.display_trees(Side.Bids)
LOB.display_trees(Side.Asks)

print("\nRemaining orders:\n")
remaining_orders = list(set(insert_orders) - set(update_orders))
for order in remaining_orders:
    print(order)
    LOB.process(order, Submit.Remove)

LOB.display_trees(Side.Bids)
LOB.display_trees(Side.Asks)


