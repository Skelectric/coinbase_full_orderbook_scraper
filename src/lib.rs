#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(unused_assignments)]

use std::fmt::{Debug, Display, Formatter};
use std::iter::{successors, zip};
use std::cmp::{max, Ordering};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ptr::{eq, NonNull};
use std::string::ToString;

type NodePtr<K, V> = NonNull<Node<K, V>>;
type Link<K, V> = Option<NodePtr<K, V>>;
type BoxedNode<K, V> = Box<Node<K, V>>;

/// AVL tree struct with a reference to the root and a node count
pub struct AVLTree<K, V> {
    root: Link<K, V>,
    len: usize,
    _boo: PhantomData<K>,
    _none: Option<NodePtr<K, V>>, // used for find_link_mut - should always be None
}

/// Node struct that stores key-value pairs and child references
#[derive(PartialEq)]
pub struct Node<K, V> {
    pub key: K,
    pub value: V,
    pub parent: Link<K, V>,
    pub left: Link<K, V>,
    pub right: Link<K, V>,
}


impl<K, V> AVLTree<K, V>
    where K: Display + Debug + PartialOrd + Clone + ToString {
    /// Create new AVL Tree
    pub fn new() -> Self {
        AVLTree { root: None, len: 0, _boo: PhantomData, _none: None}
    }

    /// Get reference to key's value
    pub fn get(&self, key: &K) -> Option<&V> {
        let link = self.find_link(&key).0;
        unsafe {
            Some(&(*((*link)?.as_ptr())).value)
        }
    }

    /// Get mutable reference to key's value
    pub fn get_mut(&self, key: &K) -> Option<&mut V> {
        let link = self.find_link(&key).0;
        unsafe {
            Some(&mut (*((*link)?.as_ptr())).value)
        }
    }

    /// Check if key is in tree
    pub fn has(&self, key: &K) -> bool {
        let link = self.find_link(key).0;
        match link {
            None => false,
            Some(node) => true,
        }
    }

    /// debugging
    fn debug_find_link(&self, key: &K) {
        println!("called debug_find_link on {}", &key);
        let (link, parent, branch) = self.find_link(key);
        unsafe {
            let link_is_some: bool = link.is_some();
            println!("link passed to debug_link is Some: {} ", &link_is_some);
            let link_key = get_key_as_str(&link);
            let parent_key = get_key_as_str(&parent);
            println!("Found link has key '{}' and parent with key '{}'", link_key, parent_key);
            match branch {
                Branch::Left => {
                    println!("Link corresponds to parent's left branch");
                    println!("Parent's left branch key is {}", (*(*parent.unwrap().as_ptr()).left.unwrap().as_ptr()).key);
                },
                Branch::Right => {
                    println!("Link corresponds to parent's right branch");
                    println!("Parent's right branch key is {}", (*(*parent.unwrap().as_ptr()).right.unwrap().as_ptr()).key);
                },
                Branch::Root => {
                    println!("Link corresponds to root branch");
                    if self.root.is_some() {
                        println!("Parent's root branch key is {}", (*self.root.unwrap().as_ptr()).key)
                    }
                }
                Branch::None => panic!()
            }
            println!();
        }
    }

    /// Get tuple containing immutable references a key's link,
    /// parent and an Enum representing the branch the key's link is in
    pub fn find_link(&self, key: &K) -> (&Link<K, V>, &Link<K, V>, Branch) {
        let mut current: &Link<K, V> = &self.root;
        let mut branch: Branch = Branch::Root;
        let mut parent: &Link<K, V> = &None;
        unsafe {
            while let Some(node_ptr) = current.as_ref() {
                let node = &mut (*node_ptr.as_ptr());  // deref the pointer
                match key.partial_cmp(&node.key) {
                    Some(Ordering::Greater) => {
                        parent = current;
                        current = &node.right;
                        branch = Branch::Right;
                    }
                    Some(Ordering::Less) => {
                        parent = current;
                        current = &node.left;
                        branch = Branch::Left;
                    },
                    _ => break,
                }
            }
        }
        (current, parent, branch)
    }

    /// Get tuple containing mutable references a key's link,
    /// parent and an Enum representing the branch the key's link is in
    pub fn find_link_mut(&mut self, key: &K) -> (&mut Link<K, V>, &mut Link<K, V>, Branch) {
        let mut current: &mut Link<K, V> = &mut self.root;
        let mut branch: Branch = Branch::Root;
        let mut parent: &mut Link<K,V> = &mut self._none;
        unsafe {
            while let Some(node_ptr) = current.as_ref() {
                let node = &mut (*node_ptr.as_ptr());
                match key.partial_cmp(&node.key) {
                    Some(Ordering::Greater) => {
                        parent = current;
                        current = &mut node.right;
                        branch = Branch::Right;
                    }
                    Some(Ordering::Less) => {
                        parent = current;
                        current = &mut node.left;
                        branch = Branch::Left;
                    },
                    _ => break,
                }
            }
        }
        (current, parent, branch)
    }

    /// Insert key-value pair into the tree
    pub fn insert(&mut self, key: K, value: V) {
        let (mut link, parent, branch) = self.find_link(&key);
        unsafe {
            if link.is_none() {  // Node doesn't exist yet, so create new
                let mut new_link: Link<K, V>
                    = Some(NonNull::new_unchecked(Box::into_raw(Box::new(
                    Node::new(key.clone(), value, *parent))))
                );

                match branch {  // Link parent's child pointers
                    Branch::Root => self.root = new_link,
                    Branch::Left => (*(parent.as_ref().unwrap().as_ptr())).left = new_link,
                    Branch::Right => (*(parent.as_ref().unwrap().as_ptr())).right = new_link,
                    Branch::None => panic!(),  // this condition should never hit
                };

                self.len += 1;
            } else {  // Node already exists, so append value
                // todo: add logic to append value to collection of values
            }
        }
    }

    /// Remove key-value pair from the tree
    pub fn remove(&mut self, key: &K) -> Option<BoxedNode<K, V>> {
        println!("\nCalled remove on {}", &key);
        //self.debug_find_link(&key);
        let (link, parent, branch) = self.find_link_mut(&key);

        print!("removed link: ");
        debug_link(&link);
        print!("removed link's parent: ");
        debug_link(&parent);

        // to store final removed node
        let removed_node: BoxedNode<K, V>;

        let mut removed_link: Link<K, V>;
        unsafe {
            let adopted_children = Node::which_children_exist(&(*(*link)?.as_ptr()));

            let removed_node_ptr = (*link)?.as_ptr();

            match adopted_children {
                Children::None => {
                    println!("removed node has no children");
                    match branch {  // Zero-out parent's child pointers
                        Branch::Root => {
                            println!("removing root's link");
                            removed_link = self.root.take();
                        },
                        Branch::Left => {
                            println!("removing parent's left link");
                            let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                            removed_link = parent_node_left.take();
                            print!("removed link's parent post-update: ");
                            debug_link(&parent);
                        },
                        Branch::Right => {
                            println!("removing parent's right link");
                            let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                            removed_link = parent_node_right.take();
                            print!("removed link's parent post-update: ");
                            debug_link(&parent);
                        },
                        Branch::None => panic!(),  // should never hit this condition
                    }
                },

                Children::Left => {  // Parent inherits removed node's left child

                    println!("removed node has left child");
                    let child = (*removed_node_ptr).left;
                    print!("child link: ");
                    debug_link(&child);

                    match branch {
                        Branch::Root => {
                            println!("removing root's link");
                            removed_link = std::mem::replace(&mut self.root, child);

                            // remove inherited child's parent to signify that the parent is root
                            let child = &mut self.root;
                            (*child.unwrap().as_ptr()).parent.take();

                            print!("child link post-update: ");
                            debug_link(&*child);
                        },

                        Branch::Left => {
                            println!("removing parent's left link");
                            let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                            removed_link = std::mem::replace(parent_node_left, child);

                            // update inherited child's parent
                            println!("updating child's parent pointer");
                            let child = &mut (*parent.unwrap().as_ptr()).left;
                            let mut child_parent = &mut (*child.unwrap().as_ptr()).parent;
                            *child_parent = *parent;

                            print!("child link post-update: ");
                            debug_link(&child);
                            print!("parent link post-update: ");
                            debug_link(&parent);
                        },

                        Branch::Right => {
                            println!("removing parent's right link");
                            let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                            removed_link = std::mem::replace(parent_node_right, child);

                            // update inherited child's parent
                            println!("updating child's parent pointer");
                            let child = &mut (*parent.unwrap().as_ptr()).right;
                            let mut child_parent = &mut (*child.unwrap().as_ptr()).parent;
                            *child_parent = *parent;

                            print!("child link post-update: ");
                            debug_link(&child);
                            print!("parent link post-update: ");
                            debug_link(&parent);
                        },

                        Branch::None => panic!(),  // should never hit this condition
                    }
                },

                Children::Right => {  // Parent inherits removed node's right child

                    println!("removed node has right child");
                    let child = (*removed_node_ptr).right;
                    print!("child link: ");
                    debug_link(&child);

                    match branch {
                        Branch::Root => {
                            println!("removing root's link");
                            removed_link = std::mem::replace(&mut self.root, child);

                            // remove inherited child's parent to signify that the parent is root
                            let child = &mut self.root;
                            (*child.unwrap().as_ptr()).parent.take();

                            print!("child link post-update: ");
                            debug_link(&*child);
                        },

                        Branch::Left => {
                            println!("removing parent's left link");
                            let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                            removed_link = std::mem::replace(parent_node_left, child);

                            // update inherited child's parent
                            println!("updating child's parent pointer");
                            let child = &mut (*parent.unwrap().as_ptr()).left;
                            let mut child_parent = &mut (*child.unwrap().as_ptr()).parent;
                            *child_parent = *parent;

                            print!("child link post-update: ");
                            debug_link(&child);
                            print!("parent link post-update: ");
                            debug_link(&parent);
                        },

                        Branch::Right => {
                            println!("removing parent's right link");
                            let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                            removed_link = std::mem::replace(parent_node_right, child);

                            // update inherited child's parent
                            println!("updating child's parent pointer");
                            let child = &mut (*parent.unwrap().as_ptr()).right;
                            let mut child_parent = &mut (*child.unwrap().as_ptr()).parent;
                            *child_parent = *parent;

                            print!("child link post-update: ");
                            debug_link(&child);
                            print!("parent link post-update: ");
                            debug_link(&parent);
                        },

                        Branch::None => panic!(),  // should never hit this condition
                    }
                },

                // Choose a successor to replace removed node
                // Removed node's parent inherits successor
                // Successor either keeps its right child or inherits the removed node's right child
                // Successor inherits removed node's left child
                // Successor's parent inherits successor's right child
                // Successor has no left child to give away
                Children::Both => {
                    println!("removed node has both children");

                    // mutable refs to link and associated node returned from successor identification
                    // these become stale when successor is moved out of tree
                    let successor_link = min_under_right(link);
                    let successor_node = &mut (*successor_link.unwrap().as_ptr());

                    // temporary variables for holding various links in between disconnection and attachment
                    let mut successor: Link<K, V>;
                    let mut removed_node_left: Link<K, V>;

                    print!("successor node: ");
                    debug_link(successor_link);

                    let successor_key = get_key(successor_link).unwrap();
                    let removed_node_right_key = get_key(&(*removed_node_ptr).right).unwrap();

                    // determine whether the successor's parent will adopt the successor's right child
                    // if successor is the removed node's right, it will remain in control of its right child
                    // otherwise, if the successor is somewhere further down the right subtree, it's right child
                    // will become the successor's parent's left child
                    if successor_key != removed_node_right_key {
                        println!("successor node is not removed node's right child");

                        removed_node_left = (*removed_node_ptr).left.take();
                        print!("removed node's left: ");
                        debug_link(&removed_node_left);

                        let removed_node_right = &mut (*removed_node_ptr).right;
                        print!("removed node's right: ");
                        debug_link(removed_node_right);

                        let successor_old_parent = &mut successor_node.parent;
                        print!("successor's old parent: ");
                        debug_link(successor_old_parent);

                        let successor_old_parent_left = &mut (*successor_old_parent.unwrap().as_ptr()).left;
                        // should be equivalent to successor at this point
                        print!("successor's old parent's left: ");
                        debug_link(successor_old_parent_left);

                        println!("Moving successor out of the tree temporarily.");
                        // replace successor's old parent's left child with the successor node's right
                        successor = std::mem::replace(successor_old_parent_left, successor_node.right);

                        print!("successor post-moveout: ");
                        debug_link(&successor);

                        print!("successor's old parent's left post-replace: ");
                        debug_link(successor_old_parent_left);

                        // update successor's right child and the right child's parent reference
                        println!("Updating successor node's right child to removed node's right child");
                        let successor_node = &mut (*successor.unwrap().as_ptr());
                        successor_node.right = *removed_node_right;
                        println!("Updating new right child's parent reference.");
                        (*successor_node.right.unwrap().as_ptr()).parent = successor;

                        print!("successor post-right-child-swap: ");
                        debug_link(&successor);
                        print!("successor's new right child: ");
                        debug_link(&successor_node.right);

                    } else {

                        println!("successor node is removed node's right child");

                        println!("popping removed node's left child into temporary variable");
                        removed_node_left = (*removed_node_ptr).left.take();
                        print!("removed node's left: ");
                        debug_link(&removed_node_left);

                        let removed_node_right = &mut (*removed_node_ptr).right;
                        print!("removed node's right: ");
                        debug_link(removed_node_right);

                        println!("popping removed node's right child (successor) into temporary variable");
                        successor = removed_node_right.take();

                    }

                    // define again
                    let successor_node = &mut (*successor.unwrap().as_ptr());

                    // update successor references and new successor's children's parent references
                    println!("connecting successor to removed node's left link");
                    successor_node.left = removed_node_left;
                    (*successor_node.left.unwrap().as_ptr()).parent = successor;

                    println!("replacing removed node with successor");
                    match branch {  // Parent inherits successor

                        Branch::Root => {
                            println!("successor is new root");
                            removed_link = std::mem::replace(&mut self.root, successor);

                            // remove successor's parent to signify that the parent is root
                            let successor = &mut self.root;
                            (*successor.unwrap().as_ptr()).parent.take();

                            print!("successor: ");
                            debug_link(&self.root);
                        },

                        Branch::Left => {
                            println!("successor is parent's left branch");
                            let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                            removed_link = std::mem::replace(parent_node_left, successor);

                            // update successor's parent
                            let successor = &mut (*parent.unwrap().as_ptr()).left;
                            let mut successor_parent = &mut (*successor.unwrap().as_ptr()).parent;
                            *successor_parent = *parent;

                            println!("successor final: ");
                            debug_link(&successor);
                            println!("parent final: ");
                            debug_link(&parent);
                        },

                        Branch::Right => {
                            println!("successor is parent's right branch");
                            let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                            removed_link = std::mem::replace(parent_node_right, successor);

                            // update successor's parent
                            let successor = &mut (*parent.unwrap().as_ptr()).right;
                            let mut successor_parent = &mut (*successor.unwrap().as_ptr()).parent;
                            *successor_parent = *parent;

                            println!("successor final: ");
                            debug_link(&successor);
                            println!("parent final: ");
                            debug_link(&parent);
                        },
                        Branch::None => panic!(),  // should never hit this condition
                    }
                }
            }
            print!("removed link: ");
            debug_link(&removed_link);
            removed_node = Box::from_raw(removed_link.unwrap().as_ptr());
        }
        self.len -= 1;
        println!("Length decreased by 1 to {}\n", self.len);
        Some(removed_node)
    }

    /// Display tree
    ///
    /// Calls the display tree method in the root node.
    pub fn display(&self) -> Vec<String> {
        let mut lines: Vec<String>;
        unsafe {
            match &self.root {
                None => {
                    lines = Vec::new()
                }
                Some(node) => {
                    lines = (*node.as_ptr()).display()
                }
            }
        }
        lines
    }
}


impl<K, V> Node<K, V>
    where K: Display + Debug + PartialOrd + Clone {
    /// Create new AVL Node
    fn new(key: K, value: V, parent: Link<K, V>) -> Self {
        Node {
            key,
            value,
            parent,
            left: None,
            right: None,
        }
    }

    /// Return which children a node has
    pub fn which_children_exist(&self) -> Children {
        match (&self.left, &self.right) {
            (None, None) => Children::None,
            (Some(_), None) => Children::Left,
            (None, Some(_)) => Children::Right,
            (Some(_), Some(_)) => Children::Both
        }
    }

    /// Get the node's key-value pair in a tuple
    pub fn items(&self) -> (&K, &V) {
        (&self.key, &self.value)
    }

    /// Get a copy of the node's key
    pub fn key(&self) -> K {
        (&self).key.clone()
    }

    /// Find whether self is left, right, or root branch of parent
    pub fn is_branch(&self) -> Branch {
        if let Some(parent) = &self.parent {
            unsafe {
                let parent_key = &(*parent.as_ptr()).key;
                match (&self.key).partial_cmp(parent_key) {
                    Some(Ordering::Greater) => Branch::Right,
                    Some(Ordering::Less) => Branch::Left,
                    _ => Branch::None,  // should never trigger
                }
            }
        } else {Branch::Root}
    }

    /// Display tree wrapper
    fn display(&self) -> Vec<String>{
        let (lines, _, _, _) = Node::display_aux(self);
        for line in &lines {
            println!("{}", line)
        };
        lines
    }

    /// Core display tree function
    fn display_aux(node: &Node<K, V>) -> (Vec<String>, usize, usize, usize) {
        // print!("display_aux - Node {} > ", &node.key);
        let children = Node::which_children_exist(&node);
        unsafe {
            match children {
                Children::None => {
                    // println!("no children >");
                    let line = format!("{:?}", &node.key);
                    let width = line.len();
                    let height = 1;
                    let middle = width / 2;
                    (vec![line], width, height, middle)
                }
                Children::Left => {
                    // println!("left child >");
                    let (lines, n, p, x) = Node::display_aux(&(*node.left.unwrap().as_ptr()));
                    // println!("back to {}", &node.key);
                    let s = format!("{:?}", &node.key);
                    let u = s.len();
                    let first_line = " ".repeat(x + 1) + &"_".repeat(n - x - 1) + &s;
                    let second_line = " ".repeat(x) + &"/" + &" ".repeat(n - x - 1 + u);
                    let mut shifted_lines: Vec<String> =
                        lines.iter().map(|line| line.to_owned() + &" ".repeat(u)).collect();
                    let mut lines = vec![first_line, second_line];
                    lines.append(&mut shifted_lines);
                    (lines, n + u, p + 2, n + u / 2)
                }
                Children::Right => {
                    // println!("right child >");
                    let (lines, n, p, x) = Node::display_aux(&(*node.right.unwrap().as_ptr()));
                    // println!("back to {}", &node.key);
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
                Children::Both => {
                    // println!("left child >");
                    let (mut left, n, p, x) = Node::display_aux(&(*node.left.unwrap().as_ptr()));
                    // println!("now {}'s right >", &node.key);
                    let (mut right, m, q, y) = Node::display_aux(&(*node.right.unwrap().as_ptr()));
                    // println!("back to {}", &node.key);
                    let s = format!("{:?}", &node.key);
                    let u = s.len();
                    let first_line = " ".repeat(x + 1) + &"_".repeat(n - x - 1) + &s + &"_".repeat(y) + &" ".repeat(m - y);
                    let second_line = " ".repeat(x) + &"/" + &" ".repeat(n - x - 1 + u + y) + &r"\" + &" ".repeat(m - y - 1);
                    let mut vector: Vec<String> = Vec::new();
                    if p < q {
                        // let spaces = " ".repeat(n);
                        for i in 0..(q - p) { vector.push(" ".repeat(n)) };
                        left.append(&mut vector)
                    } else if q < p {
                        for i in 0..(p - q) { vector.push(" ".repeat(m)) };
                        right.append(&mut vector)
                    }
                    let zipped_lines: Vec<(String, String)> = zip(left, right).collect();
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
}

/// Return non-empty link with the smallest key that's greater than the passed link's key
/// i.e. take one step right and then step left until the end
/// Will return the same link that was passed if it has no right child or is empty
/// This is a separate function instead of a method because of borrow-checker
fn min_under_right<K, V>(link: &mut Link<K, V>) -> &mut Link<K, V> {
    let mut minimum = link;
    // continue if passed link is non-empty
    if let Some(node) = minimum {
        unsafe {
            // continue if passed link has non-empty right child
            if (&(*node.as_ptr()).right).is_some() {
                minimum = &mut (*node.as_ptr()).right;
                // loop until left child is none
                while (*minimum.unwrap().as_ptr()).left.is_some() {
                    minimum = &mut (*minimum.unwrap().as_ptr()).left
                }
            }
        }
    }
    minimum
}

/// Return non-empty link with the greatest key that's lesser than the passed link's key
/// i.e. take one step left and then step right until the end
/// Will return the same link that was passed if it has no left child or is empty
/// /// This is a separate function instead of a method because of borrow-checker
fn max_under_left<K, V>(link: &mut Link<K, V>) -> &mut Link<K, V> {
    let mut maximum = link;
    // continue if passed link is non-empty
    if let Some(node) = maximum {
        unsafe {
            // continue if passed link has non-empty left child
            if (&(*node.as_ptr()).left).is_some() {
                maximum = &mut (*node.as_ptr()).left;
                // loop until right child is none
                while (*maximum.unwrap().as_ptr()).right.is_some() {
                    maximum = &mut (*maximum.unwrap().as_ptr()).right
                }
            }
        }
    }
    maximum
}

/// Get key from link
fn get_key<K, V>(link: &Link<K, V>) -> Option<K>
    where K: Clone {
    unsafe {
        Some((*(*link)?.as_ptr()).key.clone())
    }
}

/// Get key as string from link - DEBUGGING
fn get_key_as_str<K, V>(link: &Link<K, V>) -> String
    where K: Clone + ToString + Debug {
    let key_option = get_key(link);
    match key_option {
        None => {
            "None".to_string()
        },
        Some(key) => {
            key.to_string()
        },
    }
}

/// debugging
fn debug_link<K, V>(link: &Link<K, V>)
    where K: Display + Clone + Debug + PartialOrd {
    match link {
        None => println!("link is empty"),
        Some(node_ptr) => {
            unsafe {
                let node = &*node_ptr.as_ptr();
                let key = &node.key;
                let parent_key = get_key_as_str(&node.parent);
                let left_key = get_key_as_str(&node.left);
                let right_key = get_key_as_str(&node.right);
                println!("key: {}, parent: {}, left: {}, right: {}", key, parent_key, left_key, right_key);
            }
        }
    }
}

#[derive(Debug)]
pub enum Branch {
    Root,
    Left,
    Right,
    None, // for debugging
}

pub enum Children {
    None,
    Left,
    Right,
    Both,
}


fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}


#[cfg(test)]
mod tests {
    use std::cmp::min;
    use rand::{Rng, seq::{IteratorRandom, SliceRandom}, distributions::uniform::SampleUniform};
    use super::*;
    use lazy_static::lazy_static;
    use std::sync::Mutex;
    use std::mem::swap;

    // lazy_static! {
    //     static ref AVL_TREE: Mutex<AVLTree<i32, Option<&'static str>>> = Mutex::new(AVLTree::new());
    //     static ref INSERT_KEYS: Mutex<Vec<i32>> = Mutex::new(vec![]);
    //     static ref TREE_UNFILLED: Mutex<bool> = Mutex::new(true);
    // }

    // fn fill_tree(){
    //     let mut run_once_flag = TREE_UNFILLED.lock().unwrap();
    //     if *run_once_flag {
    //         *run_once_flag = false;
    //         print!("Filling test AVL tree with keys... ");
    //         let mut rng = rand::thread_rng();
    //         // let keys: Vec<i32> = vec![29, 1, 9, 36, 48, 40, 46, 76, 79, 1, 94, 53, 29, 97, 83];
    //         let mut keys: Vec<i32> = (0..15).map(|_| rng.gen_range(0..100)).collect();
    //
    //         let mut tree = AVL_TREE.lock().unwrap();
    //         for key in &keys {
    //             let link = tree.insert(key.clone(), None);
    //             assert!(link.as_ref().is_some());
    //             // println!("Key {} inserted, returned key = {}", key, link.as_ref().unwrap().key);
    //         }
    //         drop(tree);
    //
    //         let mut tmp_keys = INSERT_KEYS.lock().unwrap();
    //         tmp_keys.append(&mut keys);
    //         println!("filled with {:?}", tmp_keys);
    //         drop(tmp_keys);
    //         drop(run_once_flag);
    //     } else { println!("fill_tree called again, but tree is already filled.")}
    // }

    fn convert(val: f32, precision: usize) -> String {
        format!("{:.prec$}", val, prec=precision)
    }

    fn get_disjoint_elements<T, R>(rng: &mut R, vector: &Vec<T>, amount: usize) -> Vec<T>
        where T: Ord + Display + SampleUniform + Copy, R: Rng + ?Sized
    {
        let max = vector.iter().fold(&vector[0], |a,b| a.max(b)).clone();
        let min = vector.iter().fold(&vector[0], |a,b| a.min(b)).clone();
        println!("generating disjoint elements between {} and {}", min, max);
        let mut elements: Vec<T> = Vec::new();
        for _ in 0..amount {
            loop {
                let rand_key = rng.gen_range(min..max);
                let contains = vector.iter().any(|&x| x==rand_key);
                println!("{} is disjoint: {}", rand_key, !contains);
                if !contains {
                    elements.push(rand_key);
                    break
                }
            };
        }
        elements
    }

    // #[test]
    // fn test_insertion_and_link_finding() {
    //     println!("\nTesting insertion and link-finding...");
    //     let mut avl_tree: AVLTree<i32, Option<&str>> = AVLTree::new();
    //     let mut rng = rand::thread_rng();
    //     // let keys: Vec<i32> = vec![29, 1, 9, 36, 48, 40, 46, 76, 79, 1, 94, 53, 29, 97, 83];
    //     let mut keys: Vec<i32> = (0..5).map(|_| rng.gen_range(0..100)).collect();
    //     // Fill tree
    //     println!("Filling tree with {:?}", keys);
    //     for key in &keys {
    //         avl_tree.insert(key.clone(), None);
    //     }
    //     avl_tree.display();
    //     // Test presence of keys
    //     let sample: Vec<i32> = keys.iter().map(|x| *x).choose_multiple(&mut rng, 5);
    //     for key in &keys {
    //         let (link, parent, branch) = avl_tree.find_link(&key);
    //         assert!(link.is_some());
    //         let link_key_option = get_key(link);
    //         let parent_key_option = get_key(parent);
    //         if let Some(parent_key) = parent_key_option {
    //             match branch {
    //                 Branch::Left => assert!(link_key_option.unwrap() < parent_key_option.unwrap()),
    //                 Branch::Right => assert!(link_key_option.unwrap() > parent_key_option.unwrap()),
    //                 _ => { }
    //             }
    //         } else {
    //             let root_key = get_key(&avl_tree.root).unwrap();
    //             assert_eq!(*key, root_key);
    //         }
    //     }
    //
    //     let disjoint: Vec<i32> = get_disjoint_elements(&mut rng, &sample, 5);
    //     println!("Testing disjoint elements: {:?}", disjoint);
    //     for key in disjoint {
    //         let (link, parent, branch) = avl_tree.find_link(&key);
    //         assert!(link.is_none());
    //     }
    //     println!();
    //
    //     // Test tree size
    //     keys.sort();
    //     keys.dedup();
    //     assert_eq!(avl_tree.len, keys.len());
    // }

    // #[test]
    // fn test_min_under_and_max_under() {
    //     println!("\nTesting min under and max under...");
    //     let mut avl_tree: AVLTree<f32, Option<Vec<&str>>> = AVLTree::new();
    //     let mut rng = rand::thread_rng();
    //     let keys: Vec<f32> = (0..15).map(|_| rng.gen_range(0..10000) as f32 / 100.0).collect();
    //     println!("Filling tree with {:?}", keys);
    //     for key in &keys {
    //         avl_tree.insert(key.clone(), None);
    //     }
    //     avl_tree.display();
    //     let sample = keys.iter().choose_multiple(&mut rng, 5);
    //     for key in sample {
    //         let (mut link, mut parent, _)
    //             = avl_tree.find_link_mut(&key);
    //
    //         unsafe {
    //             let link_next = min_under_right(&mut link);
    //             let link_next_key = &(*link_next.unwrap().as_ptr()).key;
    //
    //             let link_prev = max_under_left(&mut link);
    //             let link_prev_key = &(*link_prev.unwrap().as_ptr()).key;
    //
    //             let parent_key_str = get_parent_key_as_str(link);
    //
    //             println!("keys before and after {}: ({}, {})", &key, &link_prev_key, &link_next_key);
    //             println!("parent for {} is {}", &key, &parent_key_str);
    //
    //         }
    //     }
    // }

    #[test]
    fn test_removals() {
        println!("\nTesting node removals...");
        let mut avl_tree: AVLTree<f32, Option<&str>> = AVLTree::new();
        let mut rng = rand::thread_rng();
        // let mut keys: Vec<f32> = (0..15).map(|_| rng.gen_range(0..10000) as f32 / 100.0).collect();
        let mut keys: Vec<f32> = vec![86.66, 22.36, 87.51, 62.35, 62.94, 11.67, 22.72, 10.06, 7.27, 18.1, 16.91, 2.96, 83.62, 17.97, 70.78];
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        keys.sort_by(|a,b| a.partial_cmp(b).unwrap());
        keys.dedup();

        let remove_keys = keys.iter().choose_multiple(&mut rng, 15);
        // let remove_keys = vec![&91.96, &37.62];
        let remove_keys_str: Vec<String> = remove_keys.iter().map(|x| convert(**x,2)).collect();
        println!("\nRemoving {}\n", remove_keys_str.join(", "));
        for key in &remove_keys {
            let node_option = avl_tree.remove(&key);
            match node_option {
                None => println!("No node to remove."),
                Some(node) => println!("Removed {:?}", node.key)
            };
            avl_tree.display();
        }
        let expected_node_count = keys.len() - remove_keys.len();
        assert_eq!(avl_tree.len, expected_node_count);
        // avl_tree.display();

        // try to call remove on empty tree
        for key in &remove_keys[..2] {
            let node_option = avl_tree.remove(&key);
            match node_option {
                None => println!("No node to remove."),
                Some(node) => println!("Removed {:?}", node.key)
            };
        }

        // shuffle keys and insert nodes again
        // keys.shuffle(&mut rng);
        let keys = vec![83.62, 11.67, 17.97, 62.94, 7.27, 10.06, 86.66, 22.36, 22.72, 2.96, 18.1, 70.78, 87.51, 62.35, 16.91];
        println!("keys shuffled, inserting again: {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        assert_eq!(avl_tree.len, keys.len());
        avl_tree.display();
        println!("\nremoving again");
        // let keys = vec![62.94];
        for key in &keys {
            avl_tree.remove(&key);
            avl_tree.display();
        }
        // assert_eq!(avl_tree.len, 0);
        // avl_tree.display();


    }
}
