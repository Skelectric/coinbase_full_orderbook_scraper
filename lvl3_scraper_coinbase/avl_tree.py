from loguru import logger
# from lvl3_scraper_coinbase.valuebook_v2 import valueList
from tools.configure_loguru import configure_logger
configure_logger()


class AVLNode:
    """AVL BST node."""
    __slots__ = ['parent', 'left', 'right', 'value', '_balance', '_height']

    def __init__(self, value):
        self.parent = None
        self.left = None
        self.right = None
        self.value = value

    @property
    def get_root(self):
        """Get the root object by moving up through parent nodes."""
        node = self
        while hasattr(node, "parent"):
            node = node.parent
        assert isinstance(node, AVLTree)
        return node

    def display_tree(self, **kwargs):
        root = self.get_root
        root.display_tree(**kwargs)

    @property
    def balance_factor(self) -> int:
        """Calculate and return the balance of this Node.
        Calculate balance by subtracting the right child's height from
        the left child's height. Children which evaluate to False (None)
        are treated as zeros."""

        right_height = self.right.height if self.right is not None else 0
        # logger.debug(f"node {self.value}'s right height = {right_height}")

        left_height = self.left.height if self.left is not None else 0
        # logger.debug(f"node {self.value}'s left height = {left_height}")

        return right_height - left_height

    @property
    def is_balanced(self) -> bool:
        """Check if node is balanced"""
        if self.balance_factor > 1 or self.balance_factor < -1:
            return False
        return True

    @property
    def grandpa(self):
        try:
            if hasattr(self, "parent"):
                return self.parent.parent
            else:
                return None
        except AttributeError:
            return None

    @property
    def height(self) -> int:
        right_height = self.right.height if self.right is not None else 0
        left_height = self.left.height if self.left is not None else 0
        if left_height > right_height:
            return left_height + 1
        else:
            return right_height + 1

    @property
    def min(self):
        """Returns the smallest node under this node."""
        minimum = self
        while minimum.left is not None:
            minimum = minimum.left
        return minimum

    @property
    def max(self):
        """Returns the largest node under this node."""
        maximum = self
        while maximum.right is not None:
            maximum = maximum.right
        return maximum

    def _replace_child_in_parent(self, node=None):
        """Replaces node in parent on a remove() call."""
        if self == self.parent.left:
            self.parent.left = node
        else:
            self.parent.right = node
        if node is not None:
            node.parent = self.parent

    def remove(self):
        """Deletes this node."""
        self.display_tree(debug=True)

        if self.left is not None and self.right is not None:  # two children

            logger.debug(f"Removed node {self.value} has 2 children.")
            parent = self.parent
            successor = self.right.min
            logger.debug(f"{successor} will replace {self}")

            # determine which node will adopt successor's right child (it will not have a left child)
            if self.right == successor:  # handle case when successor is removed node's right child
                right_adopter = successor
                if successor.right is not None:
                    logger.debug(f"{successor} will remain as {successor.right}'s parent")
            else:
                right_adopter = successor.parent
                if successor.right is not None:
                    logger.debug(f"{right_adopter} will adopt {successor.right}")

            # swap references
            successor.parent, successor.left = self.parent, self.left

            # Update references for ancestors and descendants
            if isinstance(parent, AVLTree) or parent.value < successor.value:
                parent.right = successor
            else:
                parent.left = successor

            # update descendants' parent references
            if right_adopter.left is not None:
                right_adopter.left.parent = right_adopter
            if successor.left is not None:
                successor.left.parent = successor
            if successor.right is not None:
                successor.right.parent = successor

            self.display_tree(debug=True)

            # balance successor or right_adopter (successor's parent),
            # whichever has the higher value (equivalent to being lower in the tree)
            if right_adopter.height < successor.value:
                right_adopter.balance()
            else:
                successor.balance()

            self.display_tree(debug=True)

        elif self.left is not None:  # only left child
            logger.debug(f"Removed node {self.value} only has left child.")
            self._replace_child_in_parent(self.left)
            self.balance()
            self.display_tree(debug=True)

        elif self.right is not None:  # only right child
            logger.debug(f"Removed node {self.value} only has right child.")
            self._replace_child_in_parent(self.right)
            self.balance()
            self.display_tree(debug=True)

        else:  # no children
            logger.debug(f"Removed node {self.value} has no children. Clearing parent's child pointer...")
            self._replace_child_in_parent()
            logger.debug(f"Now balancing...")
            self.balance()
            self.display_tree(debug=True)

    def balance_parent(self):
        """Checks if our parent needs balancing."""
        if isinstance(self.parent, AVLTree):  # if parent is root, balance is complete
            logger.debug(f"Parent is root, balancing complete.")
            logger.debug(f"...")
            pass
        else:
            self.parent.balance()

    def balance(self):
        """Call the rotation method relevant to this Node's balance factor.
         This call works itself up the tree recursively."""
        logger.debug(f"Checking balance of {self}")

        if self.balance_factor > 1:  # right is too heavy
            if self.right.balance_factor < 0:  # right's left is heavier, RL case
                logger.debug(f"Rotating nodes for RL Case.")
                self._rl_case()
            elif self.right.balance_factor >= 0:  # right's right is heavier, RR case
                logger.debug(f"Rotating nodes for RR Case.")
                self._rr_case()
            self.display_tree(debug=True)
        elif self.balance_factor < -1:  # left is too heavy
            if self.left.balance_factor <= 0:  # left's left is heavier, LL case
                logger.debug(f"Rotating nodes for LL Case.")
                self._ll_case()
            elif self.left.balance_factor > 0:  # left's right is heavier, LR case
                logger.debug(f"Rotating nodes for LR Case.")
                self._lr_case()
            self.display_tree(debug=True)
        else:
            pass

        self.balance_parent()

    def _ll_case(self):
        """Rotate Nodes for LL Case.
        Reference:
            https://en.wikipedia.org/wiki/File:Tree_Rebalancing.gif"""
        child, grand_child = self.left, self.left.right  # identify child and grandchild affected
        child.parent, child.right = self.parent, self  # update pointers for child

        if grand_child is not None:
            grand_child.parent = self

        if isinstance(self.parent, AVLTree) or self.value > self.parent.value:
            self.parent.right = child
        else:
            self.parent.left = child

        self.parent, self.left = child, grand_child  # update pointers for self

    def _rr_case(self):
        """Rotate Nodes for RR Case.
        Reference:
            https://en.wikipedia.org/wiki/File:Tree_Rebalancing.gif"""
        child, grand_child = self.right, self.right.left  # identify child and grandchild affected
        child.parent, child.left = self.parent, self  # update pointers for child

        if grand_child is not None:
            grand_child.parent = self

        if isinstance(self.parent, AVLTree) or self.value > self.parent.value:
            self.parent.right = child
        else:
            self.parent.left = child

        self.parent, self.right = child, grand_child  # update pointers for self

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
            self.left, self.left.right, self.parent

        logger.debug(f"grand_child pre-update - {grand_child}")
        logger.debug(f"child pre-update - {child}")
        logger.debug(f"self pre-update - {self}")
        logger.debug(f"ancestor pre-update - {parent}")
        logger.debug(f"grand_child's left child pre-update - {grand_child.left}")
        logger.debug(f"grand_child's right child pre-update - {grand_child.right}")

        # Inside References (self, child, grand_child)
        # update grandchild's pointers
        # update child's pointers (left child should be unchanged)
        # update self pointers (right child should be unchanged)

        grand_child.parent, grand_child.left, grand_child.right, \
        child.parent, child.right, \
        self.parent, self.left \
            = \
            self.parent, child, self, \
            grand_child, grand_child.left, \
            grand_child, grand_child.right

        logger.debug(f"final top - {grand_child}")
        logger.debug(f"final left - {child}")
        logger.debug(f"final right - {self}")

        # Outside References (ancestors, descendants)
        # update ancestor's child reference
        if parent.is_root or parent.value < grand_child.value:
            parent.right = grand_child
        else:
            parent.left = grand_child
        logger.debug(f"ancestor final - {parent}")

        # update descendants' parent reference
        if child.right is not None:
            child.right.parent = child
        if self.left is not None:
            self.left.parent = self

        logger.debug(f"final left, right - {grand_child.left}")
        logger.debug(f"final right, left - {grand_child.right}")

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
            self.right, self.right.left, self.parent

        self.display_tree(debug=True)

        # Inside References (self, child, grand_child)
        # update grandchild's pointers
        # update child's pointers (right child should be unchanged)
        # update self pointers (left child should be unchanged)

        grand_child.parent, grand_child.left, grand_child.right, \
        child.parent, child.left, \
        self.parent, self.right \
            = \
            self.parent, self, child, \
            grand_child, grand_child.right, \
            grand_child, grand_child.left

        # Outside References (ancestors, descendants)
        # update ancestor's child pointers
        if isinstance(parent, AVLTree) or parent.value < grand_child.value:
            parent.right = grand_child
        else:
            parent.left = grand_child

        # update descendants' parent pointers
        if child.left is not None:
            child.left.parent = child
        if self.right is not None:
            self.right.parent = self

        self.display_tree(debug=True)

    def get_child_count(self):
        node_count = 0
        if self.right is None and self.left is None:
            return node_count
        if self.right is None:
            node_count += 1
            node_count += self.left.get_child_count()
            return node_count
        if self.left is None:
            node_count += 1
            node_count += self.right.get_child_count()
            return node_count
        node_count += 2
        node_count += self.left.get_child_count()
        node_count += self.right.get_child_count()
        return node_count

    def check_reference_validity(self, raise_errors=False, msg_container: set = None) -> None | set:
        """Check that children values follow left/right rule."""
        if self.left is not None:

            # check value validity
            msg = f"self.value = {self.value}, self.left.value = {self.left.value}"
            if raise_errors:
                assert self.left.value < self.value, msg
            else:
                if self.left.value >= self.value:
                    msg = "Invalid branching found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            # check value validity
            msg = f"self.value = {self.value}, self.left.parent.value = {self.left.parent.value}"
            if raise_errors:
                assert self.value == self.left.parent.value, msg
            else:
                if self.value != self.left.parent.value:
                    msg = "Invalid parent/child references found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            self.left.check_reference_validity(raise_errors=raise_errors, msg_container=msg_container)

        if self.right is not None:

            # check value validity
            msg = f"self.value = {self.value}, self.right.value = {self.right.value}"
            if raise_errors:
                assert self.right.value > self.value, msg
            else:
                if self.right.value <= self.value:
                    msg = "Invalid branching found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            # check parent validity
            msg = f"self.value = {self.value}, self.right.parent.value = {self.right.parent.value}"
            if raise_errors:
                assert self.value == self.right.parent.value, msg
            else:
                if self.value != self.right.parent.value:
                    msg = "Invalid parent/child references found: " + msg
                    if msg_container is not None:
                        msg_container.add(msg)
                    else:
                        logger.warning(msg)

            self.right.check_reference_validity(raise_errors=raise_errors, msg_container=msg_container)

        if msg_container is not None:
            return msg_container

    def __str__(self):
        s = f'Node({self.value}/h:{self.height}/b:{self.balance_factor})'
        return s

    def __repr__(self):
        l_value = self.left.value if self.left is not None else None
        r_value = self.right.value if self.right is not None else None
        p_value = self.parent.value if self.parent is not None else None

        s = f'Node({self.value}'
        s += f'/l:{l_value}' if l_value is not None else ''
        s += f'/r:{r_value}' if r_value is not None else ''
        s += f'/p:{p_value}/h:{self.height}/b:{self.balance_factor})'
        return s

    def __len__(self):
        pass
        # todo:


class AVLTree:
    """AVL BST Root Node."""
    __slots__ = ["right", "value"]

    def __init__(self):
        # BST attributes
        self.right = None
        self.value = 'Root'

    @property
    def height(self):
        if self.right is not None:
            return self.right.height + 1

    def insert(self, value) -> AVLNode:
        """Iterative AVL Insert method to insert a new value."""
        current_node = self
        logger.debug(f"Inserting object with value {value}")

        while True:
            if isinstance(current_node, AVLTree) or value > current_node.value:
                if current_node.right is None:  # create new node in AVL tree to add value into
                    node = AVLNode(value)
                    current_node.right = node
                    current_node.right.parent = current_node  # set new node's parent
                    logger.debug(f"Inserted new {current_node.right}")
                    self.display_tree(debug=True)
                    logger.debug(f"Balancing parents...")
                    current_node.right.balance_parent()
                    self.display_tree()  # debugging
                    return node
                else:
                    current_node = current_node.right
                    continue

            elif value < current_node.value:
                if current_node.left is None:  # create new node in AVL tree to add value into
                    node = AVLNode(value)
                    current_node.left = node
                    current_node.left.parent = current_node  # set new node's parent
                    logger.debug(f"Inserted new {current_node.left}")
                    self.display_tree(debug=True)
                    logger.debug(f"Balancing parents...")
                    current_node.left.balance_parent()
                    self.display_tree()  # debugging
                    return node
                else:
                    current_node = current_node.left
                    continue

            else:  # the level already exists
                current_node.append(value)
                return current_node

    def remove(self, key):
        """Iterative method to remove an existing Node."""
        logger.debug(f"Removing node with key {key}")
        try:
            _node = self.get_node(key)
        except MissingNodeException:
            return None
        else:
            _node.remove()
            return _node

    def get_node(self, key) -> AVLNode:
        """Iterative method to find an existing Node using key"""
        current_node = self
        while True:
            if isinstance(current_node, AVLTree) or key > current_node.value:
                current_node = current_node.right
                continue
            elif key < current_node.value:
                current_node = current_node.left
                continue
            elif key == current_node.value:
                logger.debug(f"Found {current_node}")
                return current_node
            else:
                raise MissingNodeException

    def check_reference_validity(self, *args, **kwargs):
        if self.right is not None:
            self.right.check_reference_validity(*args, **kwargs)

    @property
    def is_balanced(self):
        if self.right is not None:
            return self.right.is_balanced
        return True

    def __len__(self):
        """Size of tree"""
        node_count = 0
        if self.right is None:
            return node_count
        else:
            node_count += 1
            node_count += self.right.get_child_count()
            return node_count

    def __str__(self):
        r_value = self.right.value if self.right is not None else None
        s = f'Root(h:{self.height})'
        return s

    def __repr__(self):
        r_value = self.right.value if self.right is not None else None
        s = f'Root('
        s += f'r:{r_value}' if r_value is not None else ''
        s += f'/h:{self.height})'
        return s

    def display_tree(self, **kwargs):
        debug = kwargs.get("debug", False)
        # noinspection PyTypeChecker
        lines, *_ = self._display_aux(self, debug=debug)

        for line in lines:
            if debug:
                logger.debug(line)
            else:
                logger.info(line)

    def _display_aux(self, _node, **kwargs):
        """Returns list of strings, width, height, and horizontal coordinate of the root."""
        debug = kwargs.get("debug", False)
        has_right = hasattr(_node, "right") and _node.right is not None
        has_left = hasattr(_node, "left") and _node.left is not None

        if not (has_right or has_left):  # No child
            line = f"{_node}" if not debug else f"{repr(_node)}"
            width = len(line)
            height = 1
            middle = width // 2
            return [line], width, height, middle

        elif not has_right:  # Only left child.
            lines, n, p, x = self._display_aux(_node.left, **kwargs)
            s = f"{_node}" if not debug else f"{repr(_node)}"
            u = len(s)
            first_line = (x + 1) * ' ' + (n - x - 1) * '_' + s
            second_line = x * ' ' + '/' + (n - x - 1 + u) * ' '
            shifted_lines = [line + u * ' ' for line in lines]
            return [first_line, second_line] + shifted_lines, n + u, p + 2, n + u // 2

        elif not has_left:  # Only right child.
            lines, n, p, x = self._display_aux(_node.right, **kwargs)
            s = f"{_node}" if not debug else f"{repr(_node)}"
            u = len(s)
            first_line = s + x * '_' + (n - x) * ' '
            second_line = (u + x) * ' ' + '\\' + (n - x - 1) * ' '
            shifted_lines = [u * ' ' + line for line in lines]
            return [first_line, second_line] + shifted_lines, n + u, p + 2, u // 2

        else:  # Two children.
            left, n, p, x = self._display_aux(_node.left, **kwargs)
            right, m, q, y = self._display_aux(_node.right, **kwargs)
            s = f"{_node}" if not debug else f"{repr(_node)}"
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


# testing
if __name__ == "__main__":
    avl_tree = AVLTree()

    # test insertion of several unique nodes
    insert_values = [5, 10, 9, 3, 11]
    nodes = []
    for _value in insert_values:
        _node = avl_tree.insert(_value)
        nodes.append(_node)
        print("active nodes: ", end='')
        print(*nodes, sep=', ')
    avl_tree.check_reference_validity()

    # test simple removal of nodes
    remove_values = [9, 11]
    removed_nodes = []
    for _value in remove_values:
        _node = avl_tree.remove(_value)
        removed_nodes.append(_node)
        nodes.remove(_node)
        print("active nodes: ", end='')
        print(*nodes, sep=', ')
        print("removed nodes: ", end='')
        print(*removed_nodes, sep=', ')
    avl_tree.check_reference_validity()

class MissingNodeException(Exception):
    pass
