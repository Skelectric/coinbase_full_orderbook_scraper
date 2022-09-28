import rust_orderbook

# {"price": "1319.26", "order_id": "76af2d47-11d1-468d-b74a-fcad11770b32",
# "remaining_size": "8.49", "type": "open", "side": "sell",
# "product_id": "ETH-USD", "time": "2022-09-27T19:31:30.869562Z", "sequence": 36673388000}

order = {
    "uid": "76af2d47-11d1-468d-b74a-fcad11770b32",
    "side": "sell",
    "size": 8.49,
    "price": 1319.26,
    "timestamp": "2022-09-27T19:31:30.869562Z"
}

LOB = rust_orderbook