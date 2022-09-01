from itertools import islice
from datetime import datetime
import copy
from loguru import logger

from lvl3_scraper_coinbase.avl_tree import LimitLevel, LimitLevelTree
from tools.configure_loguru import configure_logger
configure_logger()


class LimitOrderBook:
    """Limit Order Book (LOB) Implementation"""
    def __init__(self):
        self.bids = LimitLevelTree()
        self.asks = LimitLevelTree()
        
        self.__bid_levels = {}  # price : size
        self.__ask_levels = {}  # price : size

        self.order_dict = {}  # order ids : order

        self.__timestamp = None

        self.error_msgs = set()

        # stats
        self.items_processed = 0
        self.tree_size_display_cutoff = 1000  # nodes

    @property
    def empty(self) -> bool:
        if len(self.bids) == 0 and len(self.asks) == 0:
            return True
        else:
            return False

    @property
    def best_bid(self):
        price = sorted(self.__bid_levels.keys(), reverse=True)[0] if self.__bid_levels != {} else None
        return price

    @property
    def best_ask(self):
        price = sorted(self.__ask_levels.keys())[0] if self.__ask_levels != {} else None
        return price

    @property
    def top_level(self):
        """Returns the best available bid and ask."""
        return self.best_bid, self.best_ask

    def process(self, order, action):
        """Processes the given order."""
        if action == "remove":
            popped_order = self.remove(order)
            return popped_order
        elif action == "change":
            self.change(order)
        elif action == "add":
            self.add(order)
        else:
            raise Exception("Unhandled order action")
        self.items_processed += 1

    def get_limit_level(self, order):
        """Get limit_level corresponding to order's price."""
        if order.is_bid:
            limit_level = self.bids.get_node(order)
        else:
            limit_level = self.asks.get_node(order)
        return limit_level

    def change(self, order):
        """Updates an existing order in the book.
        It also updates the order's related LimitLevel's size, accordingly."""
        try:
            self.order_dict[order.uid].size = order.size
        except KeyError:
            return None

        size_diff = self.order_dict[order.uid].size - order.size
        self.order_dict[order.uid].parent_limit.size -= size_diff

        # change size of price level
        if order.is_bid:
            self.__bid_levels[self.order_dict[order.uid].price] -= size_diff
        else:
            self.__ask_levels[self.order_dict[order.uid].price] -= size_diff

    def remove(self, order):
        """Removes an order from the book.
        If the Limit Level is then empty, it is also removed from the book's relevant tree.
        If the removed LimitLevel was either the top bid or ask, it is replaced
        by the next best value."""
        self.__timestamp = order.timestamp

        # Remove Order from self.orders
        try:
            popped_order = self.order_dict.pop(order.uid)
        except KeyError:
            # logger.info("Closed order id was not found in orders dict.")
            return None

        # Remove order from its doubly linked list
        # logger.debug(f"Removing order from DLL: {popped_order}")
        popped_order.pop_from_list()

        # reduce size of price level
        if popped_order.is_bid:
            self.__bid_levels[popped_order.price] -= popped_order.size
        else:
            self.__ask_levels[popped_order.price] -= popped_order.size

        # get corresponding limit_level and order_list
        limit_level = self.get_limit_level(popped_order)
        order_list = limit_level.orders

        # Remove price level from set and update best bid or best ask
        if order_list.count == 0:
            # logger.debug(f"Root order list has 0 orders remaining.")

            if popped_order.is_bid:
                self.__bid_levels.pop(popped_order.price)
            else:
                self.__ask_levels.pop(popped_order.price)

            assert isinstance(limit_level, LimitLevel)
            limit_level.remove()

            # logger.debug(f"Removed node from tree.")

        return popped_order

    def add(self, order):
        """Inserts order into AVL tree and updates best bid and best ask."""
        self.order_dict[order.uid] = order
        self.__timestamp = order.timestamp

        # insert order into tree and update bid_levels/ask_levels
        if order.is_bid:
            self.bids.insert(order)

            if order.price not in self.__bid_levels:
                self.__bid_levels[order.price] = order.size
            else:
                self.__bid_levels[order.price] += order.size

        else:
            self.asks.insert(order)

            if order.price not in self.__ask_levels:
                self.__ask_levels[order.price] = order.size
            else:
                self.__ask_levels[order.price] += order.size

    @property
    def levels(self, depth=None) -> tuple[dict, dict]:

        if depth is not None:

            bids = []
            asks = []

            if self.__bid_levels != {}:
                bids = list(islice(self.__bid_levels.keys(), depth))
                bids.sort(reverse=True)

            if self.__ask_levels != {}:
                asks = list(islice(self.__ask_levels.keys(), depth))
                asks.sort()

            bid_levels = {price: size for price, size in self.__bid_levels.items() if price in bids}
            ask_levels = {price: size for price, size in self.__ask_levels.items() if price in asks}

        else:

            bid_levels = copy.deepcopy(self.__bid_levels)
            ask_levels = copy.deepcopy(self.__ask_levels)

        return bid_levels, ask_levels

    def sorted_levels(self, bids: bool = True):
        if bids:
            return {k: self.__bid_levels[k] for k in sorted(self.__bid_levels.keys(), reverse=True)}
        else:
            return {k: self.__ask_levels[k] for k in sorted(self.__ask_levels.keys())}


    @property
    def timestamp(self, datetime_format=False):
        return self.__timestamp

    def display_bid_tree(self):
        logger.info(f"Bids AVL Tree (size: {len(self.bids)}, height: {self.bids.height})")
        self.bids.display_tree()

    def display_ask_tree(self):
        logger.info(f"Asks AVL Tree (size: {len(self.asks)}, height: {self.asks.height})")
        self.asks.display_tree()

    def check(self, raise_errors=False):

        # Check for consistency with AVL trees
        # logger.debug(f"Checking levels against AVL trees...")
        levels = self.levels
        if raise_errors:
            assert len(self.bids) == len(levels[0])
            assert len(self.asks) == len(levels[1])
        else:
            bid_tree_size, ask_tree_size = len(self.bids), len(self.asks)
            bid_levels_size, ask_levels_size = len(levels[0]), len(levels[1])
            # logger.debug(f"bid tree size = {bid_tree_size}, bid_levels_size = {bid_levels_size}, {'OK.' if bid_levels_size==bid_tree_size else 'Mismatch!'}")
            # logger.debug(f"ask tree size = {ask_tree_size}, ask_levels_size = {ask_levels_size}, {'OK.' if ask_levels_size == ask_tree_size else 'Mismatch!'}")

        # Check that all pointers within AVL trees are correct
        # logger.debug(f"Checking pointer validity...")
        self.bids.check_reference_validity(raise_errors=raise_errors, msg_container=self.error_msgs)
        self.asks.check_reference_validity(raise_errors=raise_errors, msg_container=self.error_msgs)

        # Check that trees were balanced successfully
        if raise_errors:
            assert self.bids.is_balanced is True
            assert self.asks.is_balanced is True
        else:
            if self.bids.is_balanced is False:
                self.error_msgs.add(f"Bids are not balanced!")
            if self.asks.is_balanced is False:
                self.error_msgs.add(f"Asks are not balanced!")

        if self.error_msgs != set():
            logger.warning('Errors found:')
            logger.info(self.error_msgs)

    def log_details(self):
        if self.error_msgs is not set():
            logger.info(f"No errors encountered.")
        else:
            logger.warning(f"******Errors encountered******")
            for msg in self.error_msgs:
                logger.warning(msg)
            logger.info('-------------------------------')

        if len(self.__bid_levels) > self.tree_size_display_cutoff:
            msg = f"Bids AVL Tree too large to display ({len(self.__bid_levels):,} nodes). "
            msg += f"Increase tree_size_display_cutoff ({self.tree_size_display_cutoff:,}) "
            msg += f"in orderbook.py to display larger trees."
            logger.info(msg)
        else:
            logger.info(f"{len(self.__bid_levels)} bid levels = {self.sorted_levels(bids=True)}:")
            self.display_bid_tree()

        if len(self.__ask_levels) > self.tree_size_display_cutoff:
            msg = f"Asks AVL Tree too large to display ({len(self.__ask_levels):,} nodes). "
            msg += f"Increase tree_size_display_cutoff ({self.tree_size_display_cutoff:,}) "
            msg += f"in orderbook.py to display larger trees."
            logger.info(msg)
        else:
            logger.info(f"{len(self.__ask_levels)} ask levels = {self.sorted_levels(bids=False)}:")
            self.display_ask_tree()

        logger.info(f"Items processed by Orderbook: {self.items_processed:,}")


class OrderList:
    """Doubly-Linked List Container Class.
    Stores head and tail orders, as well as count.
    Keeps a reference to its parent LimitLevel Instance.
    This container was added because it makes deleting the LimitLevels easier.
    Has no other functionality."""
    __slots__ = ["head", "tail", "parent_limit", "size", "count"]

    def __init__(self, parent_limit):
        self.head = None
        self.tail = None
        self.count = 0
        self.size = 0
        self.parent_limit = parent_limit

    def __len__(self):
        return self.count

    def append(self, order):
        """Appends an order to this List.
        Same as LimitLevel append, except it automatically updates head and tail
        if it's the first order in this list."""
        if not self.tail:
            order.root = self
            self.tail = order
            self.head = order
            self.count += 1
        else:
            self.tail.append(order)

    def __str__(self):
        string = f"OrderList (self.head={self.head}\nself.tail={self.tail}\n"
        string += f"self.count={self.count}, self.parent_limit={self.parent_limit}"
        return string


class Order:
    """Doubly-Linked List Order item.
    Keeps a reference to root, as well as previous and next order in line.
    It also performs any and all updates to the root's tail, head and count
    references, as well as updating the related LimitLevel's size, whenever
    a method is called on this instance.
    Offers append() and pop() methods. Prepending isn't implemented."""
    __slots__ = ["uid", "is_bid", "size", "price", "timestamp", "next_item", "previous_item", "root"]

    def __init__(self, uid, size=None, is_bid=None, price=None, root=None,
                 timestamp=None, next_item=None, previous_item=None):
        # Data values
        self.uid = uid
        self.is_bid = is_bid
        self.size = size
        self.price = price
        self.timestamp = timestamp if timestamp is not None else datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # DLL attributes
        self.next_item = next_item
        self.previous_item = previous_item
        self.root = root

    @property
    def parent_limit(self):
        return self.root.parent_limit

    @property
    def get_root(self):
        return self.root

    def append(self, order):
        """Append an order.
        :param order: Order() instance
        """
        if self.next_item is None:
            self.next_item = order
            self.next_item.previous_item = self
            self.next_item.root = self.root

            # Update Root Statistics in OrderList root obj
            self.root.count += 1
            self.root.tail = order

            self.parent_limit.size += order.size

        else:
            self.next_item.append(order)

    def pop_from_list(self):
        """Pops this item from the DoublyLinkedList it belongs to."""
        if self.previous_item is None:  # if no prev item, then we are head
            self.root.head = self.next_item  # next item is new head
            if self.next_item:
                self.next_item.previous_item = None

        if self.next_item is None:  # if no next item, then we are tail
            self.root.tail = self.previous_item  # prev item is new tail
            if self.previous_item:
                self.previous_item.next_item = None

        self.root.count -= 1
        self.parent_limit.size -= self.size

        return self.__repr__()

    def __str__(self):
        return str(
            (
                f"Order id: {self.uid}",
                "bid" if self.is_bid else "ask",
                f"Price: {self.price}",
                f"Size: {self.size}",
                self.timestamp,
                f"Next Order: {self.next_item.uid if self.next_item is not None else None}",
                f"Previous Order: {self.previous_item.uid if self.previous_item is not None else None}",
                f"Inserted into OrderList = {True if self.root is not None else False}"
            )
        )

    def __repr__(self):
        return str(
            (
                self.uid,
                self.is_bid,
                self.price,
                self.size,
                self.timestamp,
                self.next_item.uid if self.next_item is not None else None,
                self.previous_item.uid if self.previous_item is not None else None
            )
        )
