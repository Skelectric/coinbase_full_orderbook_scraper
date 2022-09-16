// mod lib;
// use lib::AVLTree;
// use rand::Rng;
//
// fn main() {
//         let mut avl_tree: AVLTree<i32, String> = AVLTree::new();
//         let mut rng = rand::thread_rng();
//         let insert_keys: Vec<i32> = (0..15).map(|_| rng.gen_range(0..100)).collect();
//         // let insert_keys: Vec<i32> = vec![5, 5, 32, 62, 12];
//         for key in &insert_keys {
//                 avl_tree.insert(key.clone(), key.to_string());
//         }
//         println!("Inserting vector into AVL tree: {:?}", insert_keys);
//         avl_tree.display();
// }