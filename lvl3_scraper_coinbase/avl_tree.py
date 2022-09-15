# built-ins
from collections.abc import Iterable
from typing import Callable
from itertools import islice
import inspect

# third-party
from loguru import logger

# homebrewr
from tools.configure_loguru import configure_logger
configure_logger()


class AVLNode:
    """AVL BST node."""
    __slots__ = ['parent', 'left', 'right', 'key', 'value_obj']

    def __init__(self, key, value=None):
        self.parent = None
        self.left = None
        self.right = None
        self.key = key  # must be sortable
        self.value_obj = value

    def append(self, value):
        """Calls value_obj's append method"""
        if hasattr(self.value_obj, "append"):
            self.value_obj.append(value)

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
        # logger.debug(f"node {self.key}'s right height = {right_height}")

        left_height = self.left.height if self.left is not None else 0
        # logger.debug(f"node {self.key}'s left height = {left_height}")

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
        if hasattr(self.parent, "left") and self == self.parent.left:
            self.parent.left = node
        else:
            self.parent.right = node
        if node is not None:
            node.parent = self.parent

    def remove(self):
        """Deletes this node."""
        self.display_tree(debug=True)

        if self.left is not None and self.right is not None:  # two children

            logger.debug(f"Removed {self} has 2 children.")
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
            if isinstance(parent, AVLTree) or parent.key < successor.key:
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
            # whichever has the higher key (equivalent to being lower in the tree)
            if right_adopter.key > successor.key:
                right_adopter.balance()
            else:
                successor.balance()

            self.display_tree(debug=True)

        elif self.left is not None:  # only left child
            logger.debug(f"Removed node {self.key} only has left child.")
            self._replace_child_in_parent(self.left)
            self.balance()
            self.display_tree(debug=True)

        elif self.right is not None:  # only right child
            logger.debug(f"Removed node {self.key} only has right child.")
            self._replace_child_in_parent(self.right)
            self.balance()
            self.display_tree(debug=True)

        else:  # no children
            logger.debug(f"Removed node {self.key} has no children. Clearing parent's child pointer...")
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

        if isinstance(self.parent, AVLTree) or self.key > self.parent.key:
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

        if isinstance(self.parent, AVLTree) or self.key > self.parent.key:
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
        if isinstance(parent, AVLTree) or parent.key < grand_child.key:
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
        if isinstance(parent, AVLTree) or parent.key < grand_child.key:
            parent.right = grand_child
        else:
            parent.left = grand_child

        # update descendants' parent pointers
        if child.left is not None:
            child.left.parent = child
        if self.right is not None:
            self.right.parent = self

        self.display_tree(debug=True)

    def __str__(self):
        s = f'Node({self.key}/h:{self.height}/b:{self.balance_factor})'
        return s

    def __repr__(self):
        l_key = self.left.key if self.left is not None else None
        r_key = self.right.key if self.right is not None else None
        p_key = self.parent.key if self.parent is not None else None

        s = f'Node({self.key}'
        s += f'/l:{l_key}' if l_key is not None else ''
        s += f'/r:{r_key}' if r_key is not None else ''
        s += f'/p:{p_key}/h:{self.height}/b:{self.balance_factor})'
        return s

    def __len__(self):
        """Get length of node's value_obj if it exists. This assumes value_obj has a valid __len__ method.
        Otherwise, return length of key (1)."""
        if hasattr(self.value_obj, "__len__"):
            return len(self.value_obj)
        else:
            return 1


class AVLTree:
    """AVL BST Root Node."""
    __slots__ = ["right", "key", "node_count"]

    def __init__(self):
        # BST attributes
        self.right = None
        self.key = 'Root'
        self.node_count = 0

    @property
    def height(self):
        if isinstance(self.right, AVLNode):
            return self.right.height + 1
        return 0

    def nodes(self, reverse=False):
        if isinstance(self.right, AVLNode):
            return [node for node in self.traverse(reverse=reverse)]
        return []

    def keys(self, reverse=False):
        if isinstance(self.right, AVLNode):
            def get_key(*args, **_):
                return args[0].key
            return [key for key in self.traverse(reverse=reverse, func=get_key)]
        return []

    @property
    def is_balanced(self):
        if isinstance(self.right, AVLNode):
            return self.right.is_balanced
        return True

    def count_nodes(self):
        if isinstance(self.right, AVLNode):
            def counter(_):
                counter.count += 1
            counter.count = 0
            for _ in self.traverse(func=counter):
                pass
            return counter.count
        return 0

    @staticmethod
    def __parse_object(obj, key_attr=None, value_attr=None) -> tuple:
        """Support insertion of sortable keys,
        iterables with first position being sortable, or classes with sortable key attributes"""

        def check_obj_sortable(_obj):
            """Check if obj is sortable, raise exception if it isn't."""
            cls = _obj.__class__
            if not hasattr(cls, "__lt__") or not hasattr(cls, "__gt__"):
                raise UnsortableObjectException(_obj)

        if key_attr is not None and value_attr is not None:
            key = getattr(obj, key_attr)
            check_obj_sortable(key)
            value = getattr(obj, value_attr)
            return key, value

        elif isinstance(obj, Iterable):
            key, *value = obj
            return key, value

        else:
            check_obj_sortable(obj)
            return obj, None

    def insert(self, obj) -> AVLNode | None:
        """Iterative AVL Insert method to insert a new value."""
        try:
            key, value = self.__parse_object(obj)
        except UnsortableObjectException as e:
            logger.warning(f"e")
            return None

        current_node = self
        logger.debug(f"Inserting object with key {key}, value {value}")

        while True:
            if isinstance(current_node, AVLTree) or key > current_node.key:
                if current_node.right is None:  # create new node in AVL tree to add value into
                    node = AVLNode(key, value)
                    current_node.right = node
                    current_node.right.parent = current_node  # set new node's parent
                    logger.debug(f"Inserted new {current_node.right}")
                    self.display_tree(debug=True)
                    logger.debug(f"Balancing parents...")
                    current_node.right.balance_parent()
                    self.display_tree()  # debugging
                    self.node_count += 1
                    return node
                else:
                    current_node = current_node.right
                    continue

            elif key < current_node.key:
                if current_node.left is None:  # create new node in AVL tree to add value into
                    node = AVLNode(key, value)
                    current_node.left = node
                    current_node.left.parent = current_node  # set new node's parent
                    logger.debug(f"Inserted new {current_node.left}")
                    self.display_tree(debug=True)
                    logger.debug(f"Balancing parents...")
                    current_node.left.balance_parent()
                    self.display_tree()  # debugging
                    self.node_count += 1
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
            node = self.get_node(key)
        except MissingNodeException:
            return None
        else:
            node.remove()  # class method for removing nodes
            self.node_count -= 1
            return node

    def get_node(self, key) -> AVLNode:
        """Iterative method to find an existing Node using key"""
        current_node = self
        while True:
            if isinstance(current_node, AVLTree):
                if current_node.right is None:
                    raise MissingNodeException
                else:
                    current_node = current_node.right
            elif key > current_node.key and current_node.right is not None:
                current_node = current_node.right
                continue
            elif key < current_node.key and current_node.left is not None:
                current_node = current_node.left
                continue
            elif key == current_node.key:
                logger.debug(f"Found {current_node}")
                return current_node
            else:
                raise MissingNodeException

    def traverse(self, reverse: bool = False, func: Callable = None, **kwargs):
        """Generator object for traversing tree. If no function supplied to func, it will yield the current node."""

        def traverse_in_order(node, __direction: tuple[str, str], __func: Callable, **f_kw):
            """LNR Traversal"""
            child1 = getattr(node, __direction[0])
            child2 = getattr(node, __direction[1])
            if child1 is not None:
                yield from traverse_in_order(child1, __direction, __func, **f_kw)
            yield __func(node, **f_kw)
            if child2 is not None:
                yield from traverse_in_order(child2, __direction, __func, **f_kw)

        def return_node(*args, **_):
            """Return node"""
            return args[0]

        # debugging to check what called this method
        # curframe = inspect.currentframe()
        # calframe = inspect.getouterframes(curframe, 2)
        # logger.debug(f"AVLTree.traverse called by: {calframe[1][3]}")

        # derive direction
        direction = ("left", "right") if not reverse else ("right", "left")

        # default to returning node when no func supplied
        func = return_node if func is None else func

        yield from traverse_in_order(self.right, direction, func, **kwargs)

    def validate(self, **kwargs) -> None:
        """Traverse over tree and validate each node's child connections.
        Pass raise_errors=True to throw Exceptions on unsuccessful validations.
        By default, unsuccessful validations will just get logged as warnings.
        Pass verbose=True to log nodes that passed validations."""

        def wrapper(node, raise_errors: bool = False, verbose: bool = False):
            def validate_branching_left(__node):
                if __node.left is not None and __node.left.key >= __node.key:
                    node_validity[__node.left] = False
                    raise InvalidBranching(__node, "left")

            def validate_branching_right(__node):
                if __node.right is not None and __node.right.key <= __node.key:
                    node_validity[__node.right] = False
                    raise InvalidBranching(__node, "right")

            def validate_parenting_left(__node):
                if __node.left is not None and __node.left.parent is not __node:
                    node_validity[__node.left] = False
                    raise InvalidParenting(__node, "left")

            def validate_parenting_right(__node):
                if __node.right is not None and __node.right.parent is not __node:
                    node_validity[__node.right] = False
                    raise InvalidParenting(__node, "right")

            def run(validation_function: Callable, __node):
                try:
                    validation_function(__node)
                except (InvalidBranching, InvalidParenting) as e:
                    nonlocal valid
                    valid = False
                    if raise_errors:
                        raise e
                    else:
                        logger.warning(e)

            run(validate_branching_left, node)
            run(validate_branching_right, node)
            run(validate_parenting_left, node)
            run(validate_parenting_right, node)

            if verbose:
                node_validity[node] = True if node_validity.get(node, None) is None else node_validity[node]

        node_validity = {}

        for _ in self.traverse(func=wrapper, **kwargs):
            pass

        for __node, valid in node_validity.items():
            logger.debug(f"{__node} OK") if valid else logger.warning(f"{__node} INVALID")

    def __len__(self):
        """Size of tree"""
        return self.node_count

    def __str__(self):
        s = f'Root(h:{self.height})'
        return s

    def __repr__(self):
        r_key = self.right.key if self.right is not None else None
        s = f'Root('
        s += f'r:{r_key}' if r_key is not None else ''
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


class MissingNodeException(Exception):
    pass


class UnsortableObjectException(Exception):
    def __init__(self, obj):
        self.message = f"{obj} is not sortable."
        super().__init__(self.message)


class InvalidBranching(Exception):
    def __init__(self, obj: AVLNode, direction: str):
        child = getattr(obj, direction)
        self.message = f"Invalid branching found. parent: {repr(obj)}, {direction}: {repr(child)}"
        super().__init__(self.message)


class InvalidParenting(Exception):
    def __init__(self, obj: AVLNode, direction: str):
        child = getattr(obj, direction)
        self.message = f"Invalid parent/child reference found. parent: {repr(obj)}, {direction}: {repr(child)}"
        super().__init__(self.message)

