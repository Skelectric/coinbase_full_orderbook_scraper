#![allow(unused_variables)]
#![allow(dead_code)]
// #![allow(unused_mut)]
#![allow(unused_imports)]
// #![allow(unused_assignments)]

use std::fmt::{Debug, Display};
use std::iter::zip;
use std::cmp::{max, Ordering};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::string::ToString;

type NodePtr<K, V> = NonNull<Node<K, V>>;
type Link<K, V> = Option<NodePtr<K, V>>;
type BoxedNode<K, V> = Box<Node<K, V>>;

/// AVL tree struct with a reference to the root and a node count
pub struct AVLTree<K, V>
    where K: Display + Debug + PartialOrd + Clone + ToString {
    root: Link<K, V>,
    len: usize,
    _boo: PhantomData<K>,
    _none: Option<NodePtr<K, V>>, // used for find_link_mut - should always be None
}

/// Node struct that stores key-value pairs and child references
#[derive(PartialEq)]
pub struct Node<K, V>
    where K: Display + Debug + PartialOrd + Clone {
    pub key: K,
    pub value: V,
    pub parent: Link<K, V>,
    pub left: Link<K, V>,
    pub right: Link<K, V>,
}

pub struct Iter<'a, K, V>
    where K: Display + Debug + PartialOrd + Clone {
    current_link: Link<K, V>,
    first_move: bool,
    len: usize,
    _boo: PhantomData<&'a K>,
}

pub enum TraversalMove {
    First,
    LeftmostInRight,
    TraversedUpFromLeft,
    TraversedUpFromRight,
}

impl<K, V> AVLTree<K, V>
    where K: Display + Debug + PartialOrd + Clone + ToString {
    /// Create new AVL Tree
    pub fn new() -> Self {
        AVLTree { root: None, len: 0, _boo: PhantomData, _none: None}
    }

    /// Get reference to key's value
    pub fn get(&self, key: &K) -> Option<&V> {
        let link = self.find_links(&key).0;
        unsafe {
            Some(&(*((*link)?.as_ptr())).value)
        }
    }

    /// Get mutable reference to key's value
    pub fn get_mut(&self, key: &K) -> Option<&mut V> {
        let link = self.find_links(&key).0;
        unsafe {
            Some(&mut (*((*link)?.as_ptr())).value)
        }
    }

    /// Check if key is in tree
    pub fn has(&self, key: &K) -> bool {
        let link = self.find_links(key).0;
        match link {
            None => false,
            Some(_) => true,
        }
    }

    /// debugging
    fn debug_find_link(&self, key: &K) {
        println!("called debug_find_link on {}", &key);
        let (link, parent, branch) = self.find_links(key);
        unsafe {
            let link_is_some: bool = link.is_some();
            println!("link passed to Self::debug_link is Some: {} ", &link_is_some);
            let link_key = Self::get_key_as_str(&link);
            let parent_key = Self::get_key_as_str(&parent);
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

    /// Get tuple containing mutable reference to a link associated with the passed key
    pub fn find_link_mut(&mut self, key: &K) -> &mut Link<K, V> {
        let mut current: &mut Link<K, V> = &mut self.root;
        unsafe {
            while let Some(node_ptr) = current.as_ref() {
                let node = &mut (*node_ptr.as_ptr());
                match key.partial_cmp(&node.key) {
                    Some(Ordering::Greater) => {
                        current = &mut node.right;
                    }
                    Some(Ordering::Less) => {
                        current = &mut node.left;
                    },
                    _ => break,
                }
            }
        }
        current
    }

    /// Get tuple containing immutable references a key's link,
    /// parent and an Enum representing the branch the key's link is in
    pub fn find_links(&self, key: &K) -> (&Link<K, V>, &Link<K, V>, Branch) {
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

    /// Get tuple containing mutable references to a key's link,
    /// parent and an Enum representing the branch the key's link is in
    pub fn find_links_mut(&mut self, key: &K) -> (&mut Link<K, V>, &mut Link<K, V>, Branch) {
        let mut current: &mut Link<K, V> = &mut self.root;
        let mut parent: &mut Link<K,V> = &mut self._none;
        let mut branch: Branch = Branch::Root;
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
        let (link, parent, branch) = self.find_links(&key);
        unsafe {
            if link.is_none() {  // Node doesn't exist yet, so create new
                let new_link: Link<K, V>
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

    /// find whether a particular link is a root, left or right branch
    fn find_branch_from_parent(link: &Link<K, V>) -> Branch {
        unsafe {
            let node = &(*link.unwrap().as_ptr());
            if node.parent.is_none() {
                Branch::Root
            } else {
                let parent_key = Self::get_key(&node.parent).unwrap();
                match node.key.partial_cmp(&parent_key) {
                    Some(Ordering::Greater) => Branch::Right,
                    Some(Ordering::Less) => Branch::Left,
                    _ => Branch::None
                }
            }
        }
    }

    /// Get link's distance from root
    pub fn distance(link: &Link<K, V>) -> usize {
        let mut distance: usize = 0;
        let mut current = link;
        unsafe {
            while let Some(parent_ptr) = current {
                current = &(*parent_ptr.as_ptr()).parent;
                distance += 1;
            }
        }
        distance
    }

    /// Get link's height
    pub fn height(link: &Link<K, V>) -> usize {
        let mut right_height: usize = 0;
        let mut left_height: usize = 0;
        unsafe {
            let node = &(*link.unwrap().as_ptr());
            if node.right.is_some() {
                right_height = AVLTree::height(&node.right);
            }
            if node.left.is_some() {
                left_height = AVLTree::height(&node.left);
            }
        };
        if left_height > right_height {
            left_height + 1
        } else {
            right_height + 1
        }
    }

    /// Get link's balance factor
    pub fn balance_factor(link: &Link<K, V>) -> isize {
        todo!()
    }


    /// flip link to a node from a child-parent reference to being a parent-child reference.
    /// Needs to be a static method to work around borrow checker in remove method.
    /// Hence, output needs to be wrapped in logic that accounts for None returns, which
    /// can either mean that flipped link is root, or that the passed link was empty.
    fn flip_link_static(link: &mut Link<K, V>) -> Option<&mut Link<K, V>> {
        let new_link: Option<&mut Link<K, V>>;
        if link.is_some() {
            let parent_branch = Self::find_branch_from_parent(&link);
            unsafe {
                let node = &mut (*link.unwrap().as_ptr());
                match parent_branch {
                    Branch::Root => new_link = None,
                    Branch::Left => {
                        let parent_node = &mut (*node.parent.unwrap().as_ptr());
                        new_link = Some(&mut parent_node.left);
                    },
                    Branch::Right => {
                        let parent_node = &mut (*node.parent.unwrap().as_ptr());
                        new_link = Some(&mut parent_node.right);
                    },
                    _ => panic!(),
                }
            }
        } else {new_link = None}
        new_link
    }

    /// print stack of keys from passed link to root. primarily for debugging
    fn trace_parentage(link: &Link<K, V>) {
        let mut stack: Vec<K> = Vec::new();
        unsafe {
            let mut current = link;
            loop {
                if current.is_some() {
                    stack.push((*current.unwrap().as_ptr()).key.clone());
                    current = &(*current.unwrap().as_ptr()).parent;
                } else {
                    break;
                }
            }
        }
        print!("Tracing parentage: ");
        for i in 0..(stack.len() - 1) {
            print!("{} > ", &stack[i]);
        }
        print!("{}\n", stack[stack.len() - 1])
    }

    /// removes the passed link from the tree, replacing its position with the
    /// child denoted by the branch parameter.
    /// returns the removed link with parent, left and right pointers cleared
    /// only works with links with a single child.
    /// method will panic if link contains more than 1 child
    fn replace_with_child(link: &mut Link<K, V>, which_child: Branch) -> Link<K, V> {
        let removed: Link<K, V>;
        unsafe {
            let node = &mut (*link.unwrap().as_ptr());

            // panic if link has more than 1 child
            match Node::which_children_exist(&node) {
                Children::Both => panic!(),
                _ => {}
            }

            // derive mutable parent link
            // should be None for root
            let mut dummy_none = None;
            let parent = AVLTree::flip_link_static(&mut node.parent);
            let parent = if parent.is_some() {
                parent.unwrap()
            } else {
                &mut dummy_none
            };

            let adopted_child: Link<K, V> = match which_child {
                Branch::Left => { node.left.take() },
                Branch::Right => { node.right.take() },
                _ => panic!(),
            };

            // debugging
            // println!("____replace with child____");
            // print!("link to be removed: ");
            // Self::debug_link(link);
            // print!("parent: ");
            // Self::debug_link(&parent);
            // print!("child that will be adopted: ");
            // Self::debug_link(&adopted_child);

            // pop link out of tree
            removed = std::mem::replace(link, adopted_child);
            // println!("------swapped------");

            // debugging
            // print!("removed_link's old location: ");
            // Self::debug_link(link);

            // update child's parent pointer if it's not None
            if link.is_some() {
                // println!("updating child's parent pointer");
                (*link.unwrap().as_ptr()).parent = *parent;
            }

            // debugging
            // print!("removed_link's old location: ");
            // Self::debug_link(link);
            // print!("removed link before clearing pointers: ");
            // Self::debug_link(&removed);
            // print!("parent: ");
            // Self::debug_link(&parent);

            // println!("--clearing removed node's pointers--");
            // clear removed link's inside pointers
            (*removed.unwrap().as_ptr()).left.take();
            (*removed.unwrap().as_ptr()).right.take();
            (*removed.unwrap().as_ptr()).parent.take();

            // debugging
            // print!("parent after clearing pointers: ");
            // Self::debug_link(&parent);
            // print!("child after clearing pointers: ");
            // Self::debug_link(link);
            // print!("removed link after clearing pointers: ");
            // Self::debug_link(&removed);
            //
            //
            // println!("___________________________");
        }
        removed
    }

    /// Remove key-value pair from the tree
    pub fn remove(&mut self, key: &K) -> Option<BoxedNode<K, V>> {
        // println!("\nCalled remove on {}", &key);
        //self.debug_find_link(&key);
        let (link_for_removal, parent, branch) = self.find_links_mut(&key);

        // let parent = NonNull::new_unchecked()

        // print!("removed link: ");
        // Self::debug_link(&link_for_removal);
        // print!("removed link's parent: ");
        // Self::debug_link(&parent);

        // to store final removed node
        let removed_node: BoxedNode<K, V>;

        let removed_link: Link<K, V>;
        unsafe {
            let adopted_children = Node::which_children_exist(&(*(*link_for_removal)?.as_ptr()));

            let removed_node_ptr = (*link_for_removal)?.as_ptr();

            match adopted_children {
                Children::None => {
                    // println!("removed node has no children");
                    removed_link = link_for_removal.take();
                    // match branch {  // Zero-out parent's child pointers
                    //
                    //     Branch::Root => {
                    //         // println!("removing root's link");
                    //         removed_link = link_for_removal.take();
                    //     },
                    //     Branch::Left => {
                    //         // println!("removing parent's left link");
                    //         removed_link = link_for_removal.take();
                    //         // print!("removed link's parent post-update: ");
                    //         // Self::debug_link(&parent);
                    //     },
                    //     Branch::Right => {
                    //         // println!("removing parent's right link");
                    //         removed_link = link_for_removal.take();
                    //         // print!("removed link's parent post-update: ");
                    //         // Self::debug_link(&parent);
                    //     },
                    //     Branch::None => panic!(),  // should never hit this condition
                    // }
                },

                Children::Left => {  // Parent inherits removed node's left child

                    // println!("removed node has left child");
                    // let child = (*removed_node_ptr).left;
                    // print!("child link: ");
                    // Self::debug_link(&child);

                    removed_link = Self::replace_with_child(link_for_removal, Branch::Left);

                    // match branch {
                    //     Branch::Root => {
                    //         // println!("removing root's link");
                    //         removed_link = Self::replace_with_child(link_for_removal, Branch::Left);
                    //     },
                    //
                    //     Branch::Left => {
                    //         // println!("removing parent's left link");
                    //         // let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                    //         removed_link = Self::replace_with_child(link_for_removal, Branch::Left);
                    //     },
                    //
                    //     Branch::Right => {
                    //         // println!("removing parent's right link");
                    //         // let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                    //         removed_link = Self::replace_with_child(link_for_removal, Branch::Left);
                    //     },
                    //
                    //     Branch::None => panic!(),  // should never hit this condition
                    // }
                },

                Children::Right => {  // Parent inherits removed node's right child

                    // println!("removed node has right child");
                    // let child = (*removed_node_ptr).right;
                    // print!("child link: ");
                    // Self::debug_link(&child);

                    removed_link = Self::replace_with_child(link_for_removal, Branch::Right)

                    // match branch {
                    //     Branch::Root => {
                    //         // println!("removing root's link");
                    //         removed_link = Self::replace_with_child(link_for_removal, Branch::Right);
                    //     },
                    //
                    //     Branch::Left => {
                    //         // println!("removing parent's left");
                    //         // let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                    //         removed_link = Self::replace_with_child(link_for_removal, Branch::Right);
                    //     },
                    //
                    //     Branch::Right => {
                    //         // println!("removing parent's right");
                    //         // let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                    //         removed_link = Self::replace_with_child(link_for_removal, Branch::Right);
                    //     },
                    //
                    //     Branch::None => panic!(),  // should never hit this condition
                    // }
                },

                // Choose a successor to replace removed node
                // Removed node's parent inherits successor
                // Successor either keeps its right child or inherits the removed node's right child
                // Successor inherits removed node's left child
                // Successor's parent inherits successor's right child
                // Successor has no left child to give away
                Children::Both => {
                    // println!("removed has both children");

                    // mutable refs to link and associated node returned from successor identification
                    // these become stale when successor is moved out of tree
                    let successor_link = Self::min_under_right(link_for_removal);
                    // let successor_node = &mut (*successor_link.unwrap().as_ptr());

                    // temporary variables for holding various links in between disconnection and attachment
                    let successor: Link<K, V>;
                    let removed_left: Link<K, V>;

                    // print!("SUCCESSOR: ");
                    // Self::debug_link(successor_link);

                    let successor_key = Self::get_key(successor_link).unwrap();
                    let removed_node_right_key = Self::get_key(&(*removed_node_ptr).right).unwrap();

                    // determine whether the successor's parent will adopt the successor's right child
                    // if successor is the removed node's right, it will remain in control of its right child
                    // otherwise, if the successor is somewhere further down the right subtree, it's right child
                    // will become the successor's parent's left child
                    if successor_key != removed_node_right_key {
                        // println!("successor node is not removed node's right child");

                        let removed_node_right = &mut (*removed_node_ptr).right;
                        // print!("removed's right: ");
                        // Self::debug_link(removed_node_right);

                        // need to flip link because successor's parent reference will get stale upon movement
                        // let successor_old_parent = Self::flip_link_static(&mut (successor_node.parent)).unwrap();

                        // print!("successor's old parent: ");
                        // Self::debug_link(&successor_old_parent);

                        // let successor_old_parent_left = &mut (*successor_old_parent.unwrap().as_ptr()).left;
                        // should be equivalent to successor at this point
                        // print!("successor's old parent's left: ");
                        // Self::debug_link(successor_old_parent_left);

                        // let successor_node_right = &mut successor_node.right;
                        // print!("successor's right: ");
                        // Self::debug_link(&successor_node_right);

                        // replace successor's old parent's left child with the successor node's right
                        successor = Self::replace_with_child(successor_link, Branch::Right);

                        // print!("successor's right: ");
                        // Self::debug_link(&successor_node_right);
                        // print!("successor_link: ");
                        // Self::debug_link(successor_link);
                        // print!("SUCCESSOR: ");
                        // Self::debug_link(&successor);

                        // let successor_old_parent = &mut successor_node.parent;

                        // print!("successor's old parent: ");
                        // Self::debug_link(successor_old_parent);
                        // print!("successor's old parent's left post-replace: ");
                        // Self::debug_link(successor_old_parent_left);
                        // print!("successor's old right post-replace: ");
                        // Self::debug_link(&successor_node_right);

                        // set successor's new right child and the update that right child's parent reference
                        // println!("-----Updating successor node's right child to removed node's right child-----");
                        let successor_node = &mut (*successor.unwrap().as_ptr());
                        successor_node.right = *removed_node_right;
                        // println!("---------Updating new right child's parent reference.--------");
                        (*successor_node.right.unwrap().as_ptr()).parent = successor;

                        // print!("successor post-right-child-swap: ");
                        // Self::debug_link(&successor);
                        // print!("successor's new right child: ");
                        // Self::debug_link(&successor_node.right);
                        //
                        // Self::trace_parentage(successor_node_right);

                    } else {

                        // println!("successor node is removed node's right child");

                        let removed_node_right = &mut (*removed_node_ptr).right;
                        // print!("removed node's right: ");
                        // Self::debug_link(removed_node_right);

                        // println!("popping removed node's right child (successor) into temporary variable");
                        successor = removed_node_right.take();

                    }

                    // alias successor's node again
                    let successor_node = &mut (*successor.unwrap().as_ptr());

                    // update successor references and new successor's children's parent references
                    // println!("popping removed node's left child into temporary variable");
                    removed_left = (*removed_node_ptr).left.take();
                    // print!("removed's left: ");
                    // Self::debug_link(&removed_left);

                    // println!("connecting successor to removed node's left link");
                    successor_node.left = removed_left;
                    (*successor_node.left.unwrap().as_ptr()).parent = successor;

                    // println!("replacing removed node with successor");
                    match branch {  // Parent inherits successor

                        Branch::Root => {
                            // println!("successor is new root");
                            removed_link = std::mem::replace(&mut self.root, successor);

                            // remove successor's parent to signify that the parent is root
                            let successor = &mut self.root;
                            (*successor.unwrap().as_ptr()).parent.take();

                            // print!("successor final: ");
                            // Self::debug_link(&self.root);
                        },

                        Branch::Left => {
                            // println!("successor is parent's left branch");
                            let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                            removed_link = std::mem::replace(parent_node_left, successor);

                            // update successor's parent
                            let successor = &mut (*parent.unwrap().as_ptr()).left;
                            let successor_parent = &mut (*successor.unwrap().as_ptr()).parent;
                            *successor_parent = *parent;

                            // println!("successor final: ");
                            // Self::debug_link(&successor);
                            // println!("parent final: ");
                            // Self::debug_link(&parent);
                        },

                        Branch::Right => {
                            // println!("successor is parent's right branch");
                            let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                            removed_link = std::mem::replace(parent_node_right, successor);

                            // update successor's parent
                            let successor = &mut (*parent.unwrap().as_ptr()).right;
                            let successor_parent = &mut (*successor.unwrap().as_ptr()).parent;
                            *successor_parent = *parent;

                            // println!("successor final: ");
                            // Self::debug_link(&successor);
                            // println!("parent final: ");
                            // Self::debug_link(&parent);
                        },

                        Branch::None => panic!(),  // should never hit this condition
                    }
                }
            }
            // print!("removed link: ");
            // Self::debug_link(&removed_link);
            removed_node = Box::from_raw(removed_link.unwrap().as_ptr());
        }
        self.len -= 1;
        // println!("Length decreased by 1 to {}\n", self.len);
        Some(removed_node)
    }

    /// Return non-empty link with the smallest key that's greater than the passed link's key
    /// i.e. take one step right and then step left until the end
    /// Will return the same link that was passed if it has no right child or is empty
    fn min_under_right(link: &mut Link<K, V>) -> &mut Link<K, V> {
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
    fn max_under_left(link: &mut Link<K, V>) -> &mut Link<K, V> {
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
    fn get_key(link: &Link<K, V>) -> Option<K> {
        unsafe {
            Some((*(*link)?.as_ptr()).key.clone())
        }
    }

    /// Get key as string from link - DEBUGGING
    fn get_key_as_str(link: &Link<K, V>) -> String {
        let key_option = Self::get_key(link);
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
    fn debug_link(link: &Link<K, V>) {
        match link {
            None => println!("link is empty"),
            Some(node_ptr) => {
                unsafe {
                    let node = &*node_ptr.as_ptr();
                    let key = &node.key;
                    let parent_key = Self::get_key_as_str(&node.parent);
                    let left_key = Self::get_key_as_str(&node.left);
                    let right_key = Self::get_key_as_str(&node.right);
                    println!("key: {}, parent: {}, left: {}, right: {}", key, parent_key, left_key, right_key);
                }
            }
        }
    }

    /// Display tree
    ///
    /// Calls the display tree method in the root node.
    pub fn display(&self) -> Vec<String> {
        let lines: Vec<String>;
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

    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            current_link: self.root,
            first_move: true,
            len: self.len,
            _boo: PhantomData,
        }
    }
}


impl<'a, K, V: 'a> Iterator for Iter<'a, K, V>
    where K: Display + Debug + PartialOrd + Clone {
    type Item = (&'a K, &'a V);

    /// In-order BST traversal
    fn next(&mut self) -> Option<Self::Item> {
        let item;
        unsafe {
            match &self.first_move {

                true => {

                    // move down to the left-most link and return its key
                    while (*self.current_link.unwrap().as_ptr()).left.is_some() {
                        self.current_link = (*self.current_link.unwrap().as_ptr()).left
                    }
                    self.first_move = false;
                    item = Some((
                        &(*self.current_link.unwrap().as_ptr()).key,
                        &(*self.current_link.unwrap().as_ptr()).value,
                    ));
                },

                false => {

                    // move down to the left-most link in the right-subtree
                    if (*self.current_link.unwrap().as_ptr()).right.is_some() {
                        self.current_link = (*self.current_link.unwrap().as_ptr()).right;

                        while (*self.current_link.unwrap().as_ptr()).left.is_some() {
                            self.current_link = (*self.current_link.unwrap().as_ptr()).left
                        }

                        // return key-value pair
                        item = Some((
                            &(*self.current_link.unwrap().as_ptr()).key,
                            &(*self.current_link.unwrap().as_ptr()).value,
                        ));

                    // otherwise, attempt to move up
                    } else {
                        loop {
                            let branch = AVLTree::find_branch_from_parent(&self.current_link);
                            match branch {
                                Branch::Root => {
                                    // if this is the root branch, then we have finished iterating over tree
                                    item = None;
                                    break;
                                }

                                Branch::Left => {
                                    // if this is the left branch, then we move up and to the right once
                                    self.current_link = (*self.current_link.unwrap().as_ptr()).parent;
                                    item = Some((
                                        &(*self.current_link.unwrap().as_ptr()).key,
                                        &(*self.current_link.unwrap().as_ptr()).value,
                                    ));
                                    break;
                                }

                                Branch::Right => {
                                    // if this is the right branch, then we continue
                                    // moving up until the branch is left or root
                                    self.current_link = (*self.current_link.unwrap().as_ptr()).parent;
                                }

                                _ => panic!(),  // This condition should never hit unless implementation is incorrect
                            }
                        }
                    }
                },
            }
        }
        item
    }
}

impl<K, V> Drop for AVLTree<K, V>
    where K: Display + Debug + PartialOrd + Clone + ToString {
    fn drop(&mut self) {
        unsafe {
            while let Some(link) = self.root {
                let key = (*link.as_ptr()).key.clone();
                self.remove(&key);
                // self.display();
            }
        }
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
                        for _ in 0..(q - p) { vector.push(" ".repeat(n)) };
                        left.append(&mut vector)
                    } else if q < p {
                        for _ in 0..(p - q) { vector.push(" ".repeat(m)) };
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

    #[test]
    fn test_insertion_and_link_finding() {
        println!("\n-----TESTING INSERTION AND LINK FINDING-----\n");
        let mut avl_tree: AVLTree<i32, Option<&str>> = AVLTree::new();
        let mut rng = rand::thread_rng();
        // let keys: Vec<i32> = vec![29, 1, 9, 36, 48, 40, 46, 76, 79, 1, 94, 53, 29, 97, 83];
        let mut keys: Vec<i32> = (0..5).map(|_| rng.gen_range(0..100)).collect();
        // Fill tree
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        // Test presence of keys
        let sample: Vec<i32> = keys.iter().map(|x| *x).choose_multiple(&mut rng, 5);
        for key in &keys {
            let (link, parent, branch) = avl_tree.find_links(&key);
            assert!(link.is_some());
            let link_key_option = AVLTree::get_key(link);
            let parent_key_option = AVLTree::get_key(parent);
            if let Some(_) = parent_key_option {
                match branch {
                    Branch::Left => assert!(link_key_option.unwrap() < parent_key_option.unwrap()),
                    Branch::Right => assert!(link_key_option.unwrap() > parent_key_option.unwrap()),
                    _ => { }
                }
            } else {
                let root_key = AVLTree::get_key(&avl_tree.root).unwrap();
                assert_eq!(*key, root_key);
            }
        }

        let disjoint: Vec<i32> = get_disjoint_elements(&mut rng, &sample, 5);
        println!("Testing disjoint elements: {:?}", disjoint);
        for key in disjoint {
            let (link, _, _) = avl_tree.find_links(&key);
            assert!(link.is_none());
        }
        println!();

        // Test tree size
        keys.sort();
        keys.dedup();
        assert_eq!(avl_tree.len, keys.len());
    }

    #[test]
    fn test_min_under_and_max_under() {
        println!("\n-----TESTING MIN UNDER AND MAX UNDER-----\n");
        let mut avl_tree: AVLTree<f32, Option<Vec<&str>>> = AVLTree::new();
        let mut rng = rand::thread_rng();
        let mut keys: Vec<f32> = (0..15).map(|_| rng.gen_range(0..10000) as f32 / 100.0).collect();
        // let mut keys = vec![81.41, 94.2, 57.01, 63.82, 55.89, 17.98, 23.98, 33.1, 92.37, 43.28, 82.44, 42.18, 58.06, 40.27, 88.83];
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        keys.sort_by(|a,b| a.partial_cmp(b).unwrap());
        keys.dedup();
        for key in &keys {
            let (link, parent, _)
                = avl_tree.find_links_mut(&key);

            unsafe {
                let link_next = AVLTree::min_under_right(link);
                let link_next_key = &(*link_next.unwrap().as_ptr()).key;

                let link_prev = AVLTree::max_under_left(link);
                let link_prev_key = &(*link_prev.unwrap().as_ptr()).key;

                let parent_key_str = AVLTree::get_key_as_str(parent);
                println!("keys before and after {}: ({}, {})", &key, &link_prev_key, &link_next_key);
                println!("parent for {} is {}", &key, &parent_key_str);
            }
        }
    }

    #[test]
    fn test_traversal() {
        println!("\n---------TESTING TREE TRAVERSAL---------\n");
        let mut avl_tree: AVLTree<f32, Option<&str>> = AVLTree::new();
        let mut rng = rand::thread_rng();
        let mut keys: Vec<f32> = (0..20).map(|_| rng.gen_range(0..10000) as f32 / 100.0).collect();
        // let mut keys: Vec<f32> = vec![39.01, 55.46, 36.21, 44.72, 9.26, 49.16, 41.26, 31.81, 35.52, 77.97, 3.66, 54.01, 73.97, 76.49, 69.61, 47.14, 6.46, 82.88, 86.19, 66.65];
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        keys.sort_by(|a,b| a.partial_cmp(b).unwrap());
        keys.dedup();

        let traversal: Vec<f32> = avl_tree.iter().map(|x| *x.0).collect();
        println!("In-order traversal: {:?}", traversal);
        for i in 0..keys.len() {
            assert_eq!(keys[i], traversal[i]);
        }
    }

    #[test]
    fn test_removals() {
        println!("\n---------TESTING NODE REMOVALS---------\n");
        let mut avl_tree: AVLTree<f32, Option<&str>> = AVLTree::new();
        let mut rng = rand::thread_rng();
        let mut keys: Vec<f32> = (0..10).map(|_| rng.gen_range(0..10000) as f32 / 100.0).collect();
        // let mut keys: Vec<f32> = vec![10.24, 87.48, 69.45, 8.28, 46.9, 50.0];
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        keys.sort_by(|a,b| a.partial_cmp(b).unwrap());
        keys.dedup();

        let remove_keys = keys.iter().choose_multiple(&mut rng, 15);
        // let remove_keys = vec![&10.24, &87.48, &8.28, &69.45, &46.9, &50.0];
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
        println!("Calling 2 removals on empty tree...");
        for key in &remove_keys[..2] {
            let node_option = avl_tree.remove(&key);
            match node_option {
                None => println!("No node to remove."),
                Some(node) => println!("Removed {:?}", node.key)
            };
        }

        // shuffle keys and insert nodes again
        keys.shuffle(&mut rng);
        println!("keys shuffled, inserting again and then removing: {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        assert_eq!(avl_tree.len, keys.len());
        for key in &keys {
            avl_tree.remove(&key);
        }
        assert_eq!(avl_tree.len, 0);
    }
}
