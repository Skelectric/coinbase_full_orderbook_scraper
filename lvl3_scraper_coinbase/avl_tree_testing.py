from loguru import logger
from avl_tree import AVLTree, AVLNode, InvalidParenting, InvalidBranching
from tools.configure_loguru import configure_logger
configure_logger(level="DEBUG")


def test_insertion_with_key_only():
    logger.critical("TEST - SIMPLE INSERTION BY KEY")

    for _key in insert_keys:
        _node = avl_tree.insert(_key)
    logger.info(f"active nodes: {avl_tree.nodes()}")

    avl_tree.validate(raise_errors=True)
    assert sorted(insert_keys) == sorted(avl_tree.keys()), f"{sorted(insert_keys)} not eq to {sorted(avl_tree.keys())}"


def test_forward_and_reverse():
    logger.critical("TEST - FORWARD AND REVERSE TRAVERSAL")
    assert sorted(insert_keys) == avl_tree.keys(), f"{sorted(insert_keys)} not eq to {avl_tree.keys()}"
    assert sorted(insert_keys)[::-1] == avl_tree.keys(reverse=True), \
        f"{sorted(insert_keys)[::-1]} not eq to {avl_tree.keys(reverse=True)}"


def test_removal_by_key():
    logger.critical("TEST - SIMPLE REMOVAL BY KEY")
    for _key in remove_keys:
        _node = avl_tree.remove(_key)
        removed_nodes.append(_node)

        logger.info(f"active nodes: {avl_tree.nodes()}")
        logger.info(f"removed nodes: {removed_nodes}")

    avl_tree.validate(raise_errors=True)
    assert sorted(list(set(insert_keys) - set(remove_keys))) == sorted(avl_tree.keys())


def test_duplicate_insertion_with_key_only():
    logger.critical("TEST - SIMPLE DUPLICATE INSERTION BY KEY")
    starting_nodes = set(avl_tree.nodes())
    key = avl_tree.keys()[0]
    _node = avl_tree.insert(key)
    assert starting_nodes == set(avl_tree.nodes())
    logger.info("OK")


def test_removal_of_all_nodes_by_key():
    logger.critical("TEST - REMOVING ALL NODES BY KEY")
    for _key in avl_tree.keys():
        _node = avl_tree.remove(_key)
        removed_nodes.append(_node)

    logger.info(f"active nodes: {avl_tree.nodes()}")
    logger.info(f"removed nodes: {removed_nodes}")
    assert avl_tree.nodes() == []


def test_removal_of_nonexisting_key():
    logger.critical("TEST - REMOVING NONEXISTING KEY")
    for _key in remove_nonexisting_key:
        _node = avl_tree.remove(_key)
        msg = f"Removed node = {_node}"
        logger.info(msg)


def test_traversal_count():
    logger.critical("TEST - TRAVERSAL COUNT")
    msg = f"Node count = {len(avl_tree)}"
    logger.info(msg)
    assert len(avl_tree) == avl_tree.count_nodes()


def test_validation():
    logger.critical("TEST - VALIDATION VIA TRAVERSAL")
    for _key in insert_keys:
        _node = avl_tree.insert(_key)
    logger.info("Testing good tree...")
    avl_tree.validate(verbose=False)
    logger.info("Testing bad tree...")
    bad_tree = generate_broken_tree()
    bad_tree.validate(verbose=False)


def generate_broken_tree() -> AVLTree:
    logger.debug("Generating broken tree for test")
    bad_tree = AVLTree()
    node1 = AVLNode(4)
    node2 = AVLNode(10)
    node3 = AVLNode(1)
    bad_tree.right = node1
    node1.left = node2
    node1.right = node3
    node2.parent = node1
    node3.parent = bad_tree
    return bad_tree


def display_tree():
    insert_keys = [72, 5, 32, 62, 12]
    for _key in insert_keys:
        _node = avl_tree.insert(_key)
        avl_tree.display_tree()


# testing
if __name__ == "__main__":
    avl_tree = AVLTree()

    insert_keys = [5, 3, 10, 9, 11]
    remove_keys = [9, 11]
    remove_nonexisting_key = [20]
    removed_nodes = []

    display_tree()

    # test_insertion_with_key_only()
    # test_forward_and_reverse()
    # test_traversal_count()
    # test_removal_by_key()
    # test_duplicate_insertion_with_key_only()
    # test_removal_of_all_nodes_by_key()
    # test_removal_of_nonexisting_key()
    # test_validation()
