from loguru import logger
from avl_tree import AVLTree, AVLNode


def test_insertion_with_key_only():
    logger.critical("TEST - SIMPLE INSERTION BY KEY")
    for _key in insert_keys:
        _node = avl_tree.insert(_key)
        msg = "active nodes: " + str(avl_tree)
        for node in avl_tree.nodes:
            msg += f", {node}"
        logger.info(msg)
    avl_tree.validate(raise_errors=True)
    assert len(avl_tree) == len(insert_keys)


def test_removal_by_key():
    logger.critical("TEST - SIMPLE REMOVAL BY KEY")
    starting_len = len(avl_tree)
    for _key in remove_keys:
        _node = avl_tree.remove(_key)
        removed_nodes.append(_node)

        msg = "active nodes: " + str(avl_tree)
        for node in avl_tree.nodes:
            msg += f", {node}"
        logger.info(msg)
        msg = "removed nodes:" + str(avl_tree)
        for node in removed_nodes:
            msg += f", {node}"
        logger.info(msg)

    avl_tree.validate(raise_errors=True)
    assert len(avl_tree) == starting_len - len(remove_keys)


def test_duplicate_insertion_with_key_only():
    logger.critical("TEST - SIMPLE DUPLICATE INSERTION BY KEY")
    key = avl_tree.nodes[0].key
    _node = avl_tree.insert(key)

    msg = "active nodes: " + str(avl_tree)
    for node in avl_tree.nodes:
        msg += f", {node}"
    logger.info(msg)
    msg = "removed nodes:" + str(avl_tree)
    for node in removed_nodes:
        msg += f", {node}"
    logger.info(msg)

    avl_tree.validate()
    print(len(avl_tree))


def test_removal_of_all_nodes_by_key():
    logger.critical("TEST - REMOVING ALL NODES BY KEY")
    for _key in avl_tree.keys:
        _node = avl_tree.remove(_key)
        removed_nodes.append(_node)

        msg = "active nodes: " + str(avl_tree)
        for node in avl_tree.nodes:
            msg += f", {node}"
        logger.info(msg)
        msg = "removed nodes:" + str(avl_tree)
        for node in removed_nodes:
            msg += f", {node}"
        logger.info(msg)

    avl_tree.validate()
    print(len(avl_tree))


def test_removal_of_nonexisting_key():
    logger.critical("TEST - REMOVING NONEXISTING KEY")
    for _key in remove_nonexisting_key:
        _node = avl_tree.remove(_key)
        msg = f"Removed node = {_node}"
        logger.info(msg)


def test_traversal():
    logger.critical("TEST - AVL TRAVERSAL")
    logger.info("Nodes in order:")
    avl_tree.traverse()
    logger.info("Nodes in reverse order:")
    avl_tree.traverse(reverse=True)


def test_traversal_count():
    logger.critical("TEST - TRAVERSAL COUNT")
    msg = f"Node count = {len(avl_tree)}"
    logger.info(msg)


def test_validation_via_traversal():
    logger.critical("TEST - VALIDATION VIA TRAVERSAL")
    logger.info("Testing good tree - Nodes in order:")
    avl_tree.validate()
    logger.info("Testing bad tree - Nodes in order:")
    bad_tree = generate_broken_tree()
    bad_tree.validate()


def generate_broken_tree() -> AVLTree:
    logger.warning("Generating broken tree for test")
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


# testing
if __name__ == "__main__":
    avl_tree = AVLTree()

    insert_keys = [5, 3, 10, 9, 11]
    remove_keys = [9, 11]
    remove_nonexisting_key = [20]
    removed_nodes = []

    test_insertion_with_key_only()
    # test_removal_by_key()
    # test_duplicate_insertion_with_key_only()
    # test_removal_of_all_nodes_by_key()
    # test_removal_of_nonexisting_key()
    test_traversal()
    test_traversal_count()
    test_validation_via_traversal()
