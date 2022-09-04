from loguru import logger
from avl_tree import AVLTree


def test_insertion_with_key_only():
    logger.critical("TEST - SIMPLE INSERTION BY KEY")
    for _key in insert_keys:
        _node = avl_tree.insert(_key)
        print("active nodes: ", end='')
        print(*avl_tree.nodes, sep=', ')
    avl_tree.validate(raise_errors=True)
    assert len(avl_tree) == len(insert_keys)


def test_removal_by_key():
    logger.critical("TEST - SIMPLE REMOVAL BY KEY")
    starting_len = len(avl_tree)
    for _key in remove_keys:
        _node = avl_tree.remove(_key)
        removed_nodes.append(_node)
        print("active nodes: ", end='')
        print(*avl_tree.nodes, sep=', ')
        print("removed nodes: ", end='')
        print(*removed_nodes, sep=', ')
    avl_tree.validate(raise_errors=True)
    assert len(avl_tree) == starting_len - len(remove_keys)


def test_duplicate_insertion_with_key_only():
    logger.critical("TEST - SIMPLY DUPLICATE INSERTION BY KEY")
    key = avl_tree.nodes[0].key
    _node = avl_tree.insert(key)
    print("active nodes: ", end='')
    print(*avl_tree.nodes, sep=', ')
    print("removed nodes: ", end='')
    print(*removed_nodes, sep=', ')
    avl_tree.validate()
    print(len(avl_tree))


def test_removal_of_all_nodes_by_key():
    logger.critical("TEST - REMOVING ALL NODES BY KEY")
    for _key in avl_tree.keys:
        _node = avl_tree.remove(_key)
        removed_nodes.append(_node)
        print("active nodes: ", end='')
        print(*avl_tree.nodes, sep=', ')
        print("removed nodes: ", end='')
        print(*removed_nodes, sep=', ')
    avl_tree.validate()
    print(len(avl_tree))


def test_removal_of_nonexisting_key():
    logger.critical("TEST - REMOVING NONEXISTING KEY")
    for _key in remove_nonexisting_key:
        _node = avl_tree.remove(_key)
        print(f"Removed node = {_node}")

def test_traversal():
    logger.critical("TEST - AVL TRAVERSAL")
    print("\nNodes in order:")
    avl_tree.traverse()
    print("\nNodes in reverse order:")
    avl_tree.traverse(reverse=True)

def test_validation_via_traversal():
    logger.critical("TEST - VALIDATION VIA TRAVERSAL")
    print("\nNodes in order:")
    avl_tree.traverse()

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
    # test_traversal()
    test_validation_via_traversal()
