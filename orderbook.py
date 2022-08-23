"""Forked from https://github.com/Crypto-toolbox/HFT-Orderbook/blob/master/lob.py"""

import time
from itertools import islice
from datetime import datetime
import copy
from loguru import logger
logger.remove()

# ======================================================================================
# Configure logger

# add file logger with full debug
# logger.add(
#     "logs\\orderbook_{time}.log", level="DEBUG", filter="orderbook",
#     format="<level>{level}</level> -- <level>{message}</level>"
# )

# ======================================================================================


class LimitOrderBook:
    """Limit Order Book (LOB) Implementation"""
    def __init__(self):
        self.bids = LimitLevelTree()
        self.asks = LimitLevelTree()
        
        self.__bid_levels = {}  # price : size
        self.__ask_levels = {}  # price : size

        self.order_dict = {}  # order ids : order

        self.__timestamp = None

        self.error_msgs = set()  # includes check failures

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
            limit_level = self.bids.find_node(order)
        else:
            limit_level = self.asks.find_node(order)
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
        self.bids.check_pointer_validity(raise_errors=raise_errors, msg_container=self.error_msgs)
        self.asks.check_pointer_validity(raise_errors=raise_errors, msg_container=self.error_msgs)

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
        if len(self.error_msgs) == 0:
            logger.info(f"No validation check failures encountered.")
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


class LimitLevel:
    """AVL BST node.
    This Binary Tree implementation balances on each insert.
    Attributes:
        parent: Parent node of this Node
        left_child: Left child of this Node; Values smaller than price
        right_child: Right child of this Node; Values greater than price
    Properties:
        height: Height of this Node
        balance: Balance factor of this Node
    """
    __slots__ = ['lob', 'price', 'size', 'parent', 'is_root', 'left_child', 'right_child', 'count', 'orders']

    def __init__(self, order):
        # Data values
        self.price = order.price
        self.size = order.size

        # BST attributes
        self.parent = None
        self.left_child = None
        self.right_child = None
        self.is_root = False

        # Doubly-linked list attributes
        self.orders = OrderList(self)
        self.append(order)

    @property
    def get_root(self):
        """Get the root LimitLevelTree object by moving up parent nodes."""
        node = self
        while not node.is_root:
            node = node.parent
        assert isinstance(node, LimitLevelTree)
        return node

    def display_tree(self):
        root = self.get_root
        root.display_tree()

    @property
    def volume(self) -> float:
        return self.price * self.size

    @property
    def balance_factor(self) -> int:
        """Calculate and return the balance of this Node.
        Calculate balance by subtracting the right child's height from
        the left child's height. Children which evaluate to False (None)
        are treated as zeros."""

        right_height = self.right_child.height if self.right_child is not None else 0
        # logger.debug(f"node {self.price}'s right_child height = {right_height}")

        left_height = self.left_child.height if self.left_child is not None else 0
        # logger.debug(f"node {self.price}'s left_child height = {left_height}")

        return right_height - left_height
    
    @property
    def is_balanced(self) -> bool:
        """Check if node is balanced"""
        if not self.is_root:
            if self.balance_factor > 1 or self.balance_factor < -1:
                return False
        return True

    @property
    def grandpa(self):
        try:
            if self.parent:
                return self.parent.parent
            else:
                return None
        except AttributeError:
            return None

    @property
    def height(self) -> int:
        right_height = self.right_child.height if self.right_child is not None else 0
        left_height = self.left_child.height if self.left_child is not None else 0
        if left_height > right_height:
            return left_height + 1
        else:
            return right_height + 1

    @property
    def min(self):
        """Returns the smallest node under this node."""
        minimum = self
        while minimum.left_child:
            minimum = minimum.left_child
        return minimum

    @property
    def max(self):
        """Returns the largest node under this node."""
        maximum = self
        while maximum.right_child:
            maximum = maximum.right_child
        return maximum

    def append(self, order):
        """Wrapper function to make appending to Order List simpler."""
        # logger.debug(f"Appending order to node {self.price}: {order}")
        return self.orders.append(order)

    def _replace_node_in_parent(self, new_value=None):
        """Replaces node in parent on a remove() call."""
        if not self.is_root:

            # logger.debug(f"self = {self.price}")
            # logger.debug(f"self.parent = {self.parent.price}")

            if self == self.parent.left_child:
                self.parent.left_child = new_value

                # debugging
                # if new_value is not None:
                #     # logger.debug(f"node {self.parent.price} has new left_child node {new_value.price}")
                # else:
                #     # logger.debug(f"node {self.parent.price}'s left_child node is now None")

            else:
                self.parent.right_child = new_value

                # debugging
                # if new_value is not None:
                #     # logger.debug(f"node {self.parent.price} has new right_child node {new_value.price}")
                # else:
                #     # logger.debug(f"node {self.parent.price}'s right_child node is now None")

        if new_value is not None:
            new_value.parent = self.parent

    def remove(self):
        """Deletes this limit level."""
        # logger.debug(f"LimitLevel.remove called on {self}.")
        # self.display_tree()

        if self.left_child is not None and self.right_child is not None:  # two children

            # logger.debug(f"Removed node {self.price} has 2 children.")
            # logger.debug(f"Finding smallest node in right subtree and largest node in left subtree.")
            parent = self.parent
            # set successor to the smallest node in right subtree
            successor = self.right_child.min
            # set adopter of successor's left_child to the greatest node in left subtree
            left_adopter = self.left_child.max
            # set adopter of successor's right_child to successor's parent
            # conditional to work around scenario where successor is the right_child
            right_adopter = successor.parent if self != successor.parent else self

            # self.display_tree()

            msg = f"Found successor node {successor.price}, left_child adopter node {left_adopter.price}"
            msg += f", right_child adopter node {right_adopter.price}."
            # logger.debug(msg)
            msg = f"Replacing removed node with successor, "
            msg += f"giving left_adopter and right_adopter respective children."
            # logger.debug(msg)

            # logger.debug(f"successor pre-update: {successor}")
            # logger.debug(f"left_adopter pre-update: {left_adopter}")
            # logger.debug(f"right_adopter pre-update: {right_adopter}")
            # logger.debug(f"parent pre-update: {parent}")
            # logger.debug(f"successor's left child pre-update: {successor.left_child}")
            # logger.debug(f"successor's right child pre-update: {successor.right_child}")

            successor.parent, successor.left_child, \
                left_adopter.right_child, \
                right_adopter.left_child, \
                = \
                self.parent, self.left_child, \
                successor.left_child, \
                successor.right_child

            # if successor is self's right_child, don't update reference
            if successor != self.right_child:
                successor.right_child = self.right_child

            # logger.debug(f"Swapped inside references.")
            # logger.debug(f"Updating descendant and ancestor references...")

            # logger.debug(f"final successor: {successor}")
            # logger.debug(f"final left_adopter: {left_adopter}")
            # logger.debug(f"final right_adopter: {right_adopter}")

            # Outside References (ancestors, descendants)
            # update ancestor's child reference
            if parent.is_root or parent.price < successor.price:
                parent.right_child = successor
            else:
                parent.left_child = successor

            # logger.debug(f"final parent: {parent}")

            # update descendants' parent references
            if left_adopter.right_child is not None:
                left_adopter.right_child.parent = left_adopter
            if right_adopter.left_child is not None:
                right_adopter.left_child.parent = right_adopter
            if successor.left_child is not None:
                successor.left_child.parent = successor
            if successor.right_child is not None:
                successor.right_child.parent = successor

            # logger.debug(f"final successor's left_child: {successor.left_child}")
            # logger.debug(f"final successor's right_child: {successor.right_child}")

            # self.display_tree()

            # logger.debug(f"Now balancing successor's previous parent node.")
            if self != right_adopter:
                right_adopter.balance()
            else:
                successor.balance()

            # self.display_tree()



        elif self.left_child is not None:  # only left child
            # logger.debug(f"Removed node {self.price} only has left child. Attempting to point parent to left child...")
            self._replace_node_in_parent(self.left_child)
            # logger.debug(f"Now balancing...")
            self.balance()
            # self.display_tree()

        elif self.right_child is not None:  # only right child
            # logger.debug(f"Removed node {self.price} only has right child. Attempting to point parent to right child...")
            self._replace_node_in_parent(self.right_child)
            # logger.debug(f"Now balancing...")
            self.balance()
            # self.display_tree()

        else:  # no children
            # logger.debug(f"Removed node {self.price} has no children. Clearing parent's child pointer...")
            self._replace_node_in_parent()
            # logger.debug(f"Now balancing...")
            self.balance()
            # self.display_tree()

    def balance_parent(self):
        """Checks if our parent needs balancing."""
        if self.parent.is_root:  # if our parent is root, we do nothing
            # logger.debug(f"Parent is root, do nothing.")
            pass
        else:  # tell grandpa to check his balance
            # logger.debug(f"Parent is not root, checking balance...")
            self.parent.balance()

    def balance_grandpa(self):
        """Checks if our grandpa needs balancing."""
        # logger.debug(f"Grandpa node of {self} is {self.grandpa}.")
        if self.grandpa is not None:
            if self.grandpa.is_root:  # if our grandpa is root, we do nothing
                # logger.debug(f"Grandpa is root, do nothing.")
                pass
            else:  # tell grandpa to check his balance
                # logger.debug(f"Grandpa is not root, checking balance...")
                self.grandpa.balance()

    def check_pointer_validity(self, raise_errors=False, msg_container: set = None) -> None | set:
        """Check that pointers are valid on all descendant nodes."""
        if self.left_child is not None:

            # check price validity
            msg = f"self.price = {self.price}, self.left_child.price = {self.left_child.price}"
            if raise_errors:
                assert self.left_child.price < self.price, msg
            else:
                if self.left_child.price >= self.price:
                    msg = "Invalid branching found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            # check parent validity
            msg = f"self.price = {self.price}, self.left_child.parent.price = {self.left_child.parent.price}"
            if raise_errors:
                assert self.price == self.left_child.parent.price, msg
            else:
                if self.price != self.left_child.parent.price:
                    msg = "Invalid parent/child references found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            self.left_child.check_pointer_validity(raise_errors=raise_errors, msg_container=msg_container)

        if self.right_child is not None:

            # check price validity
            msg = f"self.price = {self.price}, self.right_child.price = {self.right_child.price}"
            if raise_errors:
                assert self.right_child.price > self.price, msg
            else:
                if self.right_child.price <= self.price:
                    msg = "Invalid branching found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            # check parent validity
            msg = f"self.price = {self.price}, self.right_child.parent.price = {self.right_child.parent.price}"
            if raise_errors:
                assert self.price == self.right_child.parent.price, msg
            else:
                if self.price != self.right_child.parent.price:
                    msg = "Invalid parent/child references found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            self.right_child.check_pointer_validity(raise_errors=raise_errors, msg_container=msg_container)

        if msg_container is not None:
            return msg_container

    def balance(self):
        """Call the rotation method relevant to this Node's balance factor.
         This call works itself up the tree recursively."""

        # logger.debug(f"Balance factor on node {self.price} = {self.balance_factor}")

        if self.balance_factor > 1:  # right is too heavy
            # logger.debug(f"Balance factor on node {self.right_child.price} = {self.right_child.balance_factor}")
            if self.right_child.balance_factor < 0:  # right_child's left is heavier, RL case
                # logger.debug(f"Rotating nodes for RL Case.")
                self._rl_case()
            elif self.right_child.balance_factor >= 0:  # right_child's right is heavier, RR case
                # logger.debug(f"Rotating nodes for RR Case.")
                self._rr_case()
            # self.display_tree()
        elif self.balance_factor < -1:  # left is too heavy
            # logger.debug(f"Balance factor on node {self.left_child.price} = {self.left_child.balance_factor}")
            if self.left_child.balance_factor <= 0:  # left_child's left is heavier, LL case
                # logger.debug(f"Rotating nodes for LL Case.")
                self._ll_case()
            elif self.left_child.balance_factor > 0:  # left_child's right is heavier, LR case
                # logger.debug(f"Rotating nodes for LR Case.")
                self._lr_case()
            # self.display_tree()
        else:
            # logger.debug(f"No balancing necessary.")
            pass

        self.balance_parent()

    def _ll_case(self):
        """Rotate Nodes for LL Case.
        Reference:
            https://en.wikipedia.org/wiki/File:Tree_Rebalancing.gif"""
        child, grand_child = self.left_child, self.left_child.right_child  # identify child and grandchild affected
        child.parent, child.right_child = self.parent, self  # update pointers for child

        if grand_child is not None:
            grand_child.parent = self

        if self.parent.is_root or self.price > self.parent.price:
            self.parent.right_child = child
        else:
            self.parent.left_child = child

        self.parent, self.left_child = child, grand_child  # update pointers for self

    def _rr_case(self):
        """Rotate Nodes for RR Case.
        Reference:
            https://en.wikipedia.org/wiki/File:Tree_Rebalancing.gif"""
        child, grand_child = self.right_child, self.right_child.left_child  # identify child and grandchild affected
        child.parent, child.left_child = self.parent, self  # update pointers for child

        if grand_child is not None:
            grand_child.parent = self

        if self.parent.is_root or self.price > self.parent.price:
            self.parent.right_child = child
        else:
            self.parent.left_child = child

        self.parent, self.right_child = child, grand_child  # update pointers for self

    def _lr_case(self):
        r"""Rotate Nodes for LR Case.
        Reference:
            https://en.wikipedia.org/wiki/File:Tree_Rebalancing.gif
                    parent____                          parent____
                              \                                   \
                        ___self                             ___grand_child___
                       /                 ----->            /                 \
                   child___                            child                 self
                          \
                      grand_child
        """
        # set child, grand_child, parent aliases
        child, grand_child, parent = \
            self.left_child, self.left_child.right_child, self.parent

        # logger.debug(f"grand_child pre-update - {grand_child}")
        # logger.debug(f"child pre-update - {child}")
        # logger.debug(f"self pre-update - {self}")
        # logger.debug(f"ancestor pre-update - {parent}")
        # logger.debug(f"grand_child's left child pre-update - {grand_child.left_child}")
        # logger.debug(f"grand_child's right child pre-update - {grand_child.right_child}")

        # Inside References (self, child, grand_child)
        # update grandchild's pointers
        # update child's pointers (left child should be unchanged)
        # update self pointers (right child should be unchanged)

        grand_child.parent, grand_child.left_child, grand_child.right_child, \
            child.parent, child.right_child, \
            self.parent, self.left_child \
            = \
            self.parent, child, self, \
            grand_child, grand_child.left_child, \
            grand_child, grand_child.right_child

        # logger.debug(f"final top - {grand_child}")
        # logger.debug(f"final left - {child}")
        # logger.debug(f"final right - {self}")

        # Outside References (ancestors, descendants)
        # update ancestor's child reference
        if parent.is_root or parent.price < grand_child.price:
            parent.right_child = grand_child
        else:
            parent.left_child = grand_child
        # logger.debug(f"ancestor final - {parent}")

        # update descendants' parent reference
        if child.right_child is not None:
            child.right_child.parent = child
        if self.left_child is not None:
            self.left_child.parent = self

        # logger.debug(f"final left, right - {grand_child.left_child}")
        # logger.debug(f"final right, left - {grand_child.right_child}")

    def _rl_case(self):
        r"""Rotate Nodes for RL Case.
        Reference:
            https://en.wikipedia.org/wiki/File:Tree_Rebalancing.gif
                    parent____                          parent____
                              \                                   \
                           self___                         ___grand_child___
                                  \        ----->         /                 \
                             ___child                   self              child
                            /
                      grand_child
        """
        # set child, grand_child, parent aliases
        child, grand_child, parent = \
            self.right_child, self.right_child.left_child, self.parent

        # logger.debug(f"grand_child pre-update - {grand_child}")
        # logger.debug(f"child pre-update - {child}")
        # logger.debug(f"self pre-update - {self}")
        # logger.debug(f"ancestor pre-update - {parent}")
        # logger.debug(f"grand_child's left child pre-update - {grand_child.left_child}")
        # logger.debug(f"grand_child's right child pre-update - {grand_child.right_child}")

        # Inside References (self, child, grand_child)
        # update grandchild's pointers
        # update child's pointers (right child should be unchanged)
        # update self pointers (left child should be unchanged)

        grand_child.parent, grand_child.left_child, grand_child.right_child, \
            child.parent, child.left_child, \
            self.parent, self.right_child \
            = \
            self.parent, self, child, \
            grand_child, grand_child.right_child, \
            grand_child, grand_child.left_child

        # logger.debug(f"final top - {grand_child}")
        # logger.debug(f"final left - {self}")
        # logger.debug(f"final right - {child}")

        # Outside References (ancestors, descendants)
        # update ancestor's child pointers
        if parent.is_root or parent.price < grand_child.price:
            parent.right_child = grand_child
        else:
            parent.left_child = grand_child
        # logger.debug(f"ancestor final - {parent}")

        # update descendants' parent pointers
        if child.left_child is not None:
            child.left_child.parent = child
        if self.right_child is not None:
            self.right_child.parent = self

        # logger.debug(f"final left, right - {grand_child.left_child}")
        # logger.debug(f"final right, left - {grand_child.right_child}")

    def get_child_count(self):
        node_count = 0
        if self.right_child is None and self.left_child is None:
            return node_count
        if self.right_child is None:
            node_count += 1
            node_count += self.left_child.get_child_count()
            return node_count
        if self.left_child is None:
            node_count += 1
            node_count += self.right_child.get_child_count()
            return node_count
        node_count += 2
        node_count += self.left_child.get_child_count()
        node_count += self.right_child.get_child_count()
        return node_count

    def __str__(self):
        l_price = self.left_child.price if self.left_child is not None else None
        l_size = round(self.left_child.size, 2) if self.left_child is not None else None
        l = f"{l_price}x{l_size}" if l_price is not None else None

        r_price = self.right_child.price if self.right_child is not None else None
        r_size = round(self.right_child.size, 2) if self.right_child is not None else None
        r = f"{r_price}x{r_size}" if r_price is not None else None

        p_price = self.parent.price if self.parent is not None else None
        p_size = round(self.parent.size, 2) if self.parent is not None else None
        p = f"{p_price}x{p_size}"

        s = f'Node: {self.price}x{round(self.size, 2)} '
        s += f'(l: {l}, r: {r}, p: {p}, h: {self.height})'

        return s

    def __len__(self):
        return len(self.orders)


class LimitLevelTree:
    """AVL BST Root Node."""
    __slots__ = ["left_child", "right_child", "is_root", "price", "size"]

    def __init__(self):
        # BST attributes
        self.left_child = None
        self.right_child = None
        self.is_root = True
        self.price = 0
        self.size = 0

    @property
    def height(self):
        if self.right_child is not None:
            return self.right_child.height

    def insert(self, order):
        """Iterative AVL Insert method to insert a new order."""
        current_node = self
        # logger.debug(f"Inserting {order}")

        while True:
            if current_node.is_root or order.price > current_node.price:
                if current_node.right_child is None:  # create new node in AVL tree to add order into
                    current_node.right_child = LimitLevel(order)
                    # logger.debug(f"Inserted order into new LimitLevel {current_node.right_child.price}")
                    current_node.right_child.parent = current_node  # set new limit level's parent
                    # self.display_tree()  # debugging
                    # logger.debug(f"Calling balance grandpa on new node.")
                    current_node.right_child.balance_grandpa()
                    break
                else:
                    current_node = current_node.right_child
                    continue

            elif order.price < current_node.price:
                if current_node.left_child is None:  # create new node in AVL tree to add order into
                    current_node.left_child = LimitLevel(order)
                    # logger.debug(f"Inserted order into new node {current_node.left_child.price}")
                    current_node.left_child.parent = current_node  # set new limit levels' parent
                    # self.display_tree()  # debugging
                    # logger.debug(f"Calling balance grandpa on new node.")
                    current_node.left_child.balance_grandpa()
                    break
                else:
                    current_node = current_node.left_child
                    continue

            else:  # the level already exists
                current_node.append(order)
                return current_node

    def remove(self, order):
        """Iterative AVL remove method to remove an order from an existing Node."""
        current_node = self
        # logger.debug(f"Removing {order}")

        while True:
            if current_node.is_root or order.price > current_node.price:
                current_node = current_node.right_child
                continue
            elif order.price < current_node.price:
                current_node = current_node.left_child
                continue
            else:
                current_node.remove(order)
                break

    def find_node(self, order):
        current_node = self
        # logger.debug(f"Looking for node {order.price}...")
        while True:
            # logger.debug(f"current_node = {current_node}")
            if current_node.is_root or order.price > current_node.price:
                current_node = current_node.right_child
                continue
            elif order.price < current_node.price:
                current_node = current_node.left_child
                continue
            else:
                return current_node

    def check_pointer_validity(self, *args, **kwargs):
        if self.right_child is not None:
            self.right_child.check_pointer_validity(*args, **kwargs)

    @property
    def is_balanced(self):
        if self.right_child is not None:
            return self.right_child.is_balanced
        return True

    def __len__(self):
        """Size of tree"""
        node_count = 0
        if self.right_child is None and self.left_child is None:
            return node_count
        if self.right_child is None:
            node_count += 1
            node_count += self.left_child.get_child_count()
            return node_count
        if self.left_child is None:
            node_count += 1
            node_count += self.right_child.get_child_count()
            return node_count
        node_count += 2
        node_count += self.left_child.get_child_count()
        node_count += self.right_child.get_child_count()
        return node_count

    def __str__(self):
        l_price = self.left_child.price if self.left_child is not None else None
        l_size = round(self.left_child.size, 2) if self.left_child is not None else None
        l = f"{l_price}x{l_size}" if l_price is not None else None

        r_price = self.right_child.price if self.right_child is not None else None
        r_size = round(self.right_child.size, 2) if self.right_child is not None else None
        r = f"{r_price}x{r_size}" if r_price is not None else None

        s = f'Node: {self.price}x{round(self.size, 2)} '
        s += f'(l: {l}, r: {r})'

        return s

    def display_tree(self):
        lines, *_ = self._display_aux(self)
        for line in lines:
            logger.info(line)
            pass

    def _display_aux(self, node):
        """Returns list of strings, width, height, and horizontal coordinate of the root."""
        # debugging
        # logger.debug(f"{node}")

        # No child
        if node.right_child is None and node.left_child is None:
            line = f"{node.price}x{round(node.size)}"
            width = len(line)
            height = 1
            middle = width // 2
            return [line], width, height, middle

        # Only left child.
        if node.right_child is None:
            lines, n, p, x = self._display_aux(node.left_child)
            s = f"{node.price}x{round(node.size)}"
            u = len(s)
            first_line = (x + 1) * ' ' + (n - x - 1) * '_' + s
            second_line = x * ' ' + '/' + (n - x - 1 + u) * ' '
            shifted_lines = [line + u * ' ' for line in lines]
            return [first_line, second_line] + shifted_lines, n + u, p + 2, n + u // 2

        # Only right child.
        if node.left_child is None:
            lines, n, p, x = self._display_aux(node.right_child)
            s = f"{node.price}x{round(node.size)}"
            u = len(s)
            first_line = s + x * '_' + (n - x) * ' '
            second_line = (u + x) * ' ' + '\\' + (n - x - 1) * ' '
            shifted_lines = [u * ' ' + line for line in lines]
            return [first_line, second_line] + shifted_lines, n + u, p + 2, u // 2

        # Two children.
        left, n, p, x = self._display_aux(node.left_child)
        right, m, q, y = self._display_aux(node.right_child)
        s = f"{node.price}x{round(node.size)}"
        u = len(s)
        first_line = (x + 1) * ' ' + (n - x - 1) * '_' + s + y * '_' + (m - y) * ' '
        second_line = x * ' ' + '/' + (n - x - 1 + u + y) * ' ' + '\\' + (m - y - 1) * ' '
        if p < q:
            left += [n * ' '] * (q - p)
        elif q < p:
            right += [m * ' '] * (p - q)
        zipped_lines = zip(left, right)
        lines = [first_line, second_line] + [a + u * ' ' + b for a, b in zipped_lines]
        return lines, n + m + u, max(p, q) + 2, n + u // 2


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
