#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use std::fmt::{Debug, Display};
use std::iter::zip;
use std::cmp::max;
use std::time::Duration;
use std::thread::sleep;

type Node<T> = Box<AVLNode<T>>;

pub struct AVLNode<T> {
    /// AVL BST Node
    pub key: Option<T>,
    pub left: Option<Node<T>>,
    pub right: Option<Node<T>>,
}

impl<T> AVLNode<T>
    where T: Display + Debug + PartialOrd{
    /// AVL Binary Tree Node
    pub fn new() -> Self {
        AVLNode {
            key: None,
            left: None,
            right: None,
        }
    }

    pub fn insert(&mut self, obj: T) -> &Option<T> {
        // Insert object into AVL tree. Returns reference to object.
        // println!("Inserting {}", &obj);
        let mut current_node = self;
        loop {
            // sleep(Duration::from_millis(1000));
            // println!("Checking node {:?}", &current_node.key.as_ref());
            if current_node.key == None {
                current_node.key = Some(obj);
                break
            } else if obj > *current_node.key.as_ref().unwrap() {
                // println!("{} > {:?} - moving right", &obj, &current_node.key.as_ref());
                match &current_node.right {
                    None => {
                        // println!("right node is None - inserting new node");
                        current_node.right = Some(Box::new(AVLNode {
                            key: Some(obj),
                            left: None,
                            right: None,
                        }));
                        current_node = &mut **current_node.right.as_mut().unwrap();
                        // println!("new node inserted. key = {}", &current_node.key.as_ref().unwrap());
                        break
                    },
                    Some(node) => {
                        // println!("right node is not None - moving to right");
                        current_node = &mut **current_node.right.as_mut().unwrap();
                        // println!("current_node set to {}", &current_node.key.as_ref().unwrap());
                        continue;
                    }
                }
            } else if obj < *current_node.key.as_ref().unwrap() {
                // println!("{} < {:?} - moving left", &obj, &current_node.key.as_ref());
                match &current_node.left {
                    None => {
                        // println!("left node is None - inserting new node");
                        current_node.left = Some(Box::new(AVLNode {
                            key: Some(obj),
                            left: None,
                            right: None,
                        }));
                        current_node = &mut **current_node.left.as_mut().unwrap();
                        // println!("new node inserted. key = {}", &current_node.key.as_ref().unwrap());
                        break
                    },
                    Some(node) => {
                        // println!("left node is not None - moving to left");
                        current_node = &mut **current_node.left.as_mut().unwrap();
                        // println!("current_node set to {}", &current_node.key.as_ref().unwrap());
                        continue
                    }
                }
            } else if obj == *current_node.key.as_ref().unwrap() {
                // println!("{} already exists in tree", &obj);
                break
            }
        };
        &current_node.key
    }

    pub fn remove(&self, key: T) -> Option<T> {
        match self.get(key) {
            None => None,
            Some(key) => {
                todo!()
            }
        }

    }

    pub fn get(&self, key: T) -> Option<&T> {
        let mut current_node = self;
        let mut result;
        loop {
            if current_node.key.as_ref() == None {
                result = None;
                break;
            } else if key > *current_node.key.as_ref().unwrap() {
                match &current_node.right {
                    None => {
                        result = None;
                        break;
                    },
                    Some(node) => {
                        current_node = &current_node.right.as_ref().unwrap();
                        continue
                    },
                }
            } else if key < *current_node.key.as_ref().unwrap() {
                match &current_node.left {
                    None => {
                        result = None;
                        break;
                    },
                    Some(node) => {
                        current_node = &current_node.left.as_ref().unwrap();
                        continue
                    }
                }
            } else {
                result = current_node.key.as_ref();
                break;
            }

        }
        result
    }

    pub fn display(&self) {
        let (lines, _, _, _) = AVLNode::display_aux(self);
        for line in lines {
            println!("{}", line)
        }
    }

    fn display_aux(node: &AVLNode<T>) -> (Vec<String>, usize, usize, usize) {
        match (&node.left, &node.right) {
            (None, None) => { // no children
                let line = format!("{:?}", &node.key.as_ref());
                let width = line.len();
                let height = 1;
                let middle = width / 2;
                (vec![line], width, height, middle)
            }
            (Some(left), None) => { // only left child
                let (lines, n, p, x) = AVLNode::display_aux(&node.left.as_ref().unwrap());
                let s = format!("{:?}", &node.key.as_ref());
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
                let (lines, n, p, x) = AVLNode::display_aux(&node.right.as_ref().unwrap());
                let s = format!("{:?}", &node.key.as_ref());
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
                let (mut left, n, p, x) = AVLNode::display_aux(&node.left.as_ref().unwrap());
                let (mut right, m, q, y) = AVLNode::display_aux(&node.right.as_ref().unwrap());
                let s = format!("{:?}", &node.key.as_ref());
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
    use super::*;
    use rand::Rng;

    #[test]
    fn test_get() {
        let mut avl_tree: AVLNode<i32> = AVLNode::new();
        let mut rng = rand::thread_rng();
        assert_eq!(avl_tree.get(5), None);
        let insert_keys: Vec<i32> = (0..15).map(|_| rng.gen_range(0..100)).collect();
        for key in &insert_keys {
            avl_tree.insert(key.clone());
        }
        for key in &insert_keys {
            assert_eq!(avl_tree.get(key.clone()), Some(key));
        }
    }

    #[test]
    fn generate_tree() {
        let mut avl_tree: AVLNode<i32> = AVLNode::new();
        let mut rng = rand::thread_rng();
        let insert_keys: Vec<i32> = (0..15).map(|_| rng.gen_range(0..100)).collect();
        // let insert_keys: Vec<i32> = vec![5, 5, 32, 62, 12];
        for key in &insert_keys {
            avl_tree.insert(key.clone());
        }
        println!("Inserting vector into AVL tree: {:?}", insert_keys);
        avl_tree.display();
    }

}
