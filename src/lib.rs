#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use std::fmt::{Debug, Display, Formatter};
use std::iter::zip;
use std::cmp::{max, Ordering};
use std::borrow::Borrow;

type Link<K, V> = Option<Box<Node<K,V>>>;

/// AVL tree struct with a reference to the root and a node count
pub struct AVLTree<K, V> {
    pub root: Link<K, V>,
    pub node_count: usize,
}

/// Node struct that stores key-value pairs and child references
#[derive(PartialEq)]
pub struct Node<K, V> {
    pub key: K,
    pub value: V,
    pub left: Link<K, V>,
    pub right: Link<K, V>,
}

impl<K, V> AVLTree<K, V>
    where K: Display + Debug + Ord + Clone {

    /// Create new AVL Tree
    pub fn new() -> Self {
        AVLTree {root : None, node_count: 0}
    }

    /// Get key-value pair in a tuple
    pub fn get(&self, key: &K) -> Option<(&K, &V)> {
        let link = self.find_link(&key);
        match link.as_ref() {
            None => None,
            Some(link) => {
                Some((&link.key, &link.value))
            }
        }
    }

    /// Get stack of immutable references to all links leading to a key.
    ///
    /// This includes keys that do not yet exist in the tree.
    pub fn get_stack(&self, key: &K) -> Vec<&Link<K, V>> {
        let mut current = &self.root;
        let mut stack: Vec<&Link<K, V>> = vec![current.clone()];
        while let Some(node) = current.as_ref() {
            match key.cmp(&node.key) {
                Ordering::Equal => break,
                Ordering::Greater => {
                    current = &node.right;
                    stack.push(current.clone());
                },
                Ordering::Less => {
                    current = &node.left;
                    stack.push(current.clone());
                }
            }
        }
        stack
    }

    /// Get reference to a parent link
    pub fn parent(&self, key: &K) -> Option<&Link<K,V>> {
        let mut stack: Vec<&Link<K,V>> = self.get_stack(key);
        stack.pop();
        if let Some(parent) = stack.pop() {
            Some(parent)
        } else {None}
    }

    /// Get mutable reference to a parent link
    pub fn parent_mut(&mut self, key: &K) -> Option<&mut Link<K,V>> {
        let parent_ref = self.parent(key);
        let tmp;
        let parent_key;

        if parent_ref.is_none() {
            return None
        } else {
            tmp = self.get_key_from_link(parent_ref.unwrap());
            parent_key = tmp.unwrap().clone();
        }

        let parent_mut = self.find_link_mut(&parent_key);
        Some(parent_mut)
    }

    /// Get key from link
    pub fn get_key_from_link(&self, link: &Link<K, V>) -> Option<K> {
        match link {
            None => None,
            Some(node) => Some(node.as_ref().key.clone())
        }
    }

    /// Get reference to a link associated with a key
    pub fn find_link(&self, key: &K) -> &Link<K,V> {
        let mut current = &self.root;
        while let Some(node) = current.as_ref() {
            match key.cmp(&node.key){
                Ordering::Equal => break,
                Ordering::Greater => current = &node.right,
                Ordering::Less => current = &node.left,
            }
        }
        current
    }

    /// Get mutable reference to a link associated with a key
    pub fn find_link_mut(&mut self, key: &K) -> &mut Link<K, V> {
        let mut current = &mut self.root;
        while current.as_mut().is_some() {
            match key.cmp(&current.as_mut().unwrap().key) {
                Ordering::Equal => break,
                Ordering::Greater => {current = &mut current.as_mut().unwrap().right}
                Ordering::Less => {current = &mut current.as_mut().unwrap().left}
            }
        }
        current
    }

    /// Insert key-value pair into the tree
    pub fn insert(&mut self, key: K, value: V) -> &mut Link<K, V>{
        let mut link = self.find_link_mut(key.borrow());
        if link.is_none() {
            let mut new_link: Link<K, V> = Some(Box::new(Node::new(key, value)));
            std::mem::swap(link, &mut new_link);
            // let mut link = self;
            // self.node_count += 1;
            // println!("Inserted {}", &link.as_ref().unwrap().key.clone());
        } else {
            // println!("{} already exists", &link.as_ref().unwrap().key.clone());
            // todo: add logic to append value to collection of values
        }
        link
    }

    /// Display tree
    ///
    /// Calls the display tree method in the root node.
    pub fn display(&self) -> Vec<String> {
        let mut lines: Vec<String>;
        match &self.root {
            None => {lines = Vec::new()},
            Some(node) => {
                lines = node.as_ref().display()
            }
        };
        lines
    }
}


impl<K, V> Node<K, V>
    where K: Display + Debug + PartialOrd{
    /// Create new AVL Node
    fn new(key: K, value: V) -> Self {
        Node {
            key,
            value,
            left: None,
            right: None,
        }
    }


    // pub fn remove(&self, key: K) -> Option<K> {
    //     match self.get(key) {
    //         None => None,
    //         Some(key) => {
    //             todo!()
    //         }
    //     }
    //
    // }
    //
    // pub fn get(&self, key: K) -> Option<&K> {
    //     let mut current_node = self;
    //     let mut result;
    //     loop {
    //         if current_node.key.as_ref() == None {
    //             result = None;
    //             break;
    //         } else if key > *current_node.key.as_ref().unwrap() {
    //             match &current_node.right {
    //                 None => {
    //                     result = None;
    //                     break;
    //                 },
    //                 Some(node) => {
    //                     current_node = &current_node.right.as_ref().unwrap();
    //                     continue
    //                 },
    //             }
    //         } else if key < *current_node.key.as_ref().unwrap() {
    //             match &current_node.left {
    //                 None => {
    //                     result = None;
    //                     break;
    //                 },
    //                 Some(node) => {
    //                     current_node = &current_node.left.as_ref().unwrap();
    //                     continue
    //                 }
    //             }
    //         } else {
    //             result = current_node.key.as_ref();
    //             break;
    //         }
    //
    //     }
    //     result
    // }

    /// Display tree wrapper
    pub fn display(&self) -> Vec<String>{
        let (lines, _, _, _) = Node::display_aux(self);
        for line in &lines {
            println!("{}", line)
        };
        lines
    }

    /// Core display tree function
    fn display_aux(node: &Node<K, V>) -> (Vec<String>, usize, usize, usize) {
        match (&node.left, &node.right) {
            (None, None) => { // no children
                let line = format!("{:?}", &node.key);
                let width = line.len();
                let height = 1;
                let middle = width / 2;
                (vec![line], width, height, middle)
            }
            (Some(left), None) => { // only left child
                let (lines, n, p, x) = Node::display_aux(&node.left.as_ref().unwrap());
                let s = format!("{:?}", &node.key);
                let u = s.len();
                let first_line = " ".repeat(x+1) + &"_".repeat(n - x - 1) + &s;
                let second_line = " ".repeat(x) + &"/" + &" ".repeat(n - x - 1 + u);
                let mut shifted_lines: Vec<String> =
                    lines.iter().map(|line| line.to_owned() + &" ".repeat(u)).collect();
                let mut lines = vec![first_line, second_line];
                lines.append(&mut shifted_lines);
                (lines, n + u, p + 2, n + u / 2)
            }
            (None, Some(right)) => { // only right child
                let (lines, n, p, x) = Node::display_aux(&node.right.as_ref().unwrap());
                let s = format!("{:?}", &node.key);
                let u = s.len();
                let first_line = s + &"_".repeat(x) + &" ".repeat(n - x);
                let second_line = " ".repeat(u + x) + &r"\" + &" ".repeat(n - x - 1);
                let mut shifted_lines: Vec<String> =
                    lines.iter().map(|line| " ".repeat(u) + line).collect();
                let mut lines = vec![first_line, second_line];
                lines.append(&mut shifted_lines);
                (lines, n + u, p + 2, u / 2)
            }
            (Some(left), Some(right)) => { // both children
                let (mut left, n, p, x) = Node::display_aux(&node.left.as_ref().unwrap());
                let (mut right, m, q, y) = Node::display_aux(&node.right.as_ref().unwrap());
                let s = format!("{:?}", &node.key);
                let u = s.len();
                let first_line = " ".repeat(x + 1) + &"_".repeat(n - x - 1) + &s + &"_".repeat(y) + &" ".repeat(m - y);
                let second_line = " ".repeat(x) + &"/" + &" ".repeat(n - x - 1 + u + y) + &r"\" + &" ".repeat(m - y - 1);
                let mut vector: Vec<String> = Vec::new();
                if p < q {
                    // let spaces = " ".repeat(n);
                    for i in 0..(q-p) { vector.push(" ".repeat(n)) };
                    left.append(&mut vector)
                } else if q < p {
                    for i in 0..(p-q) { vector.push(" ".repeat(m)) };
                    right.append(&mut vector)
                }
                let zipped_lines: Vec<(String,String)>= zip(left, right).collect();
                let mut zipped_lines: Vec<String> = zipped_lines
                    .iter()
                    .map(|a| a.0.clone() + &" ".repeat(u) + &a.1.clone())
                    .collect();
                let mut lines = vec![first_line, second_line];
                lines.append(&mut zipped_lines);
                (lines, n + m + u, max(p, q) + 2, n + u / 2)
            }
        }
    }
}


fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}


#[cfg(test)]
mod tests {
    use rand::distributions::uniform::SampleUniform;
    use rand::Rng;
    use super::*;
    use lazy_static::lazy_static;
    use std::sync::Mutex;
    use std::mem::swap;

    lazy_static! {
        static ref AVL_TREE: Mutex<AVLTree<i32, Option<&'static str>>> = Mutex::new(AVLTree::new());
        static ref INSERT_KEYS: Mutex<Vec<i32>> = Mutex::new(vec![]);
        static ref TREE_UNFILLED: Mutex<bool> = Mutex::new(true);
    }

    fn fill_tree_only_once(){
        let mut flag = TREE_UNFILLED.lock().unwrap();
        if *flag {
            let mut rng = rand::thread_rng();
            // let keys: Vec<i32> = vec![29, 1, 9, 36, 48, 40, 46, 76, 79, 1, 94, 53, 29, 97, 83];
            let mut keys: Vec<i32> = (0..15).map(|_| rng.gen_range(0..100)).collect();

            let mut tree = AVL_TREE.lock().unwrap();
            for key in &keys {
                let link = tree.insert(key.clone(), None);
                assert!(link.as_ref().is_some());
                // println!("Key {} inserted, returned key = {}", key, link.as_ref().unwrap().key);
            }
            drop(tree);

            let mut tmp_keys = INSERT_KEYS.lock().unwrap();
            tmp_keys.append(&mut keys);
            println!("Filled test AVL tree with keys: {:?}", tmp_keys);
            drop(tmp_keys);
            *flag = false;
            drop(flag)
        }
    }

    fn get_random_element<T>(vector: &Vec<T>) -> T
        where T: Copy
    {
        let mut rng = rand::thread_rng();
        vector[rng.gen_range(0..vector.len())].clone()
    }

    fn get_disjoint_element<T>(vector: &Vec<T>) -> T
        where T: Ord + Display + SampleUniform + Copy
    {
        let mut rng = rand::thread_rng();
        let max = vector.iter().fold(&vector[0], |a,b| a.max(b)).clone();
        let min = vector.iter().fold(&vector[0], |a,b| a.min(b)).clone();
        let mut x;
        loop {
            x = rng.gen_range(min..max);
            if !vector.contains(&x) {
                break
            }
        };
        x
    }

    #[test]
    fn test_parents() {
        fill_tree_only_once();
        let insert_keys_temp = INSERT_KEYS.lock().unwrap();
        let mut tree_temp = AVL_TREE.lock().unwrap();

        // test parent
        println!("Retrieving parents of all elements in tree...");
        for key in &*insert_keys_temp {
            let parent_link = tree_temp.parent(&key);
            let parent_key: Option<&i32> = match parent_link {
                None => None,
                Some(link) => {
                    match link {
                        None => None,
                        Some(node) => {
                            Some(&node.as_ref().key)
                        }
                    }
                }
            };
            println!("Parent of node {} = {:?}", &key, parent_key);
        }

        // test parent_mut
        println!("Retrieving mutable parents of all elements in tree...");
        for key in &*insert_keys_temp {
            let parent_mut_link_option = tree_temp.parent_mut(&key);
            if parent_mut_link_option.is_some() {
                let mut new_link: Link<i32, Option<&str>>
                    = Some(Box::new(Node::new(key.clone(), Some("yeet"))));
                let x = parent_mut_link_option.unwrap();
                swap(x, &mut new_link);
            }
        }


        drop(insert_keys_temp);
        drop(tree_temp);
    }

    #[test]
    fn test_inserts_and_gets() {
        fill_tree_only_once();

        // test that node count is correct
        // insert_keys.sort();
        // insert_keys.dedup();
        // assert_eq!(&insert_keys.len(), &avl_tree.node_count);

        let tree = AVL_TREE.lock().unwrap();
        tree.display();
        drop(tree);

        // test retrieval
        let mut key;
        let mut link;
        let insert_keys_temp = INSERT_KEYS.lock().unwrap();
        let mut stack;
        let tree_temp = AVL_TREE.lock().unwrap();
        for _ in 0..10 {
            let mut rng = rand::thread_rng();
            let exists = rng.gen_bool(0.50);

            // test find_link
            if exists {
                key = get_random_element(&insert_keys_temp);
                link = tree_temp.find_link(&key);
                assert!(link.is_some());
            } else {
                key = get_disjoint_element(&insert_keys_temp);
                link = tree_temp.find_link(&key);
                assert!(link.is_none());
            }
        }

        // test get_stack
        key = get_random_element(&insert_keys_temp);
        stack = tree_temp.get_stack(&key);
        println!("Printing stack to key {}", &key);
        for link in stack {
            match link {
                None => print!("None"),
                Some(node) => print!("{} > ", node.as_ref().key),
            }
        }
        print!("\n");

        drop(insert_keys_temp);
        drop(tree_temp);
    }
}
