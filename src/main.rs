use avl_tree::AVLNode;

fn main() {
    let mut avl_tree: AVLNode<i32> = AVLNode::new();
    avl_tree.insert(10000000);
    avl_tree.insert(20000000);
    avl_tree.insert(0);
    avl_tree.display();
}