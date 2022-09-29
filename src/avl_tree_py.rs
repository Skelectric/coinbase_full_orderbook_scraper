#![allow(unused_variables)]
#![allow(dead_code)]
// #![allow(unused_mut)]
#![allow(unused_imports)]
// #![allow(unused_assignments)]

// standard library
use std::fmt::{Debug, Display};
use std::iter::zip;
use std::cmp::{max, Ordering};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::string::ToString;
use std::ops::Index;
// homebrew
use crate::orderbook_py::*;

pub trait Remove { fn remove(&mut self, index: usize) -> Self; }
pub trait New { fn new() -> Self; }

type NodePtr<K> = NonNull<Node<K>>;
type Link<K> = Option<NodePtr<K>>;
type LinkPtr<K> = NonNull<Link<K>>;
type BoxedNode<K> = Box<Node<K>>;

/// AVL tree struct with a reference to the root and a node count
pub struct AVLTree<K>
    where K: Display + Debug + PartialOrd + Clone + ToString, {
    root: Link<K>,
    len: usize,
    _boo: PhantomData<K>,
    _none: Option<NodePtr<K>>, // used for find_link_mut - should always be None
}

/// Node struct that stores key-value pairs and child references
// #[derive(PartialEq)]
pub struct Node<K>
    where K: Display + Debug + PartialOrd + Clone {
    pub key: K,
    pub value: OrderStack,
    pub parent: Link<K>,
    pub left: Link<K>,
    pub right: Link<K>,
}

pub struct Iter<'a, K>
    where K: Display + Debug + PartialOrd + Clone {
    current_link: Link<K>,
    first_move: bool,
    len: usize,
    _boo: PhantomData<&'a K>,
}

#[derive(PartialEq)]
pub enum LinkRotation {
    LLCase,
    RRCase,
    LRCase,
    RLCase,
}

pub enum LinkLocation<K>
    where K: Display + Debug + Clone + PartialOrd {
    None {
        parent: Link<K>,
        link_ptr: LinkPtr<K>,
    },
    Some {
        parent: Link<K>,
        link_ptr: LinkPtr<K>,
    }
}

impl<K> AVLTree<K>
    where K: Display + Debug + PartialOrd + Clone + ToString {
    /// Create new AVL Tree
    pub fn new() -> Self {
        AVLTree { root: None, len: 0, _boo: PhantomData, _none: None}
    }

    /// Return count of nodes in tree
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get reference to key's value
    pub fn get(&self, key: &K) -> Option<&OrderStack> {
        let link = self.find_link(&key);
        unsafe {
            Some(&(*((*link)?.as_ptr())).value)
        }
    }

    /// Get mutable reference to key's value
    pub fn get_mut(&self, key: &K) -> Option<&mut OrderStack> {
        let link = self.find_link(&key);
        unsafe {
            Some(&mut (*((*link)?.as_ptr())).value)
        }
    }

    /// Check if key is in tree
    pub fn has(&self, key: &K) -> bool {
        let link = self.find_link(key);
        match link {
            None => false,
            Some(_) => true,
        }
    }

    /// Return if tree is balanced
    pub fn is_balanced(&self) -> bool {
        let balance = Self::balance_factor(&self.root) as i32;
        let balanced_range = -1..=1;
        balanced_range.contains(&balance)
    }

    /// Get immutable reference to a link associated with the passed key
    fn find_link(&self, key: &K) -> &Link<K> {
        let mut current: &Link<K> = &self.root;
        unsafe {
            while let Some(node_ptr) = current.as_ref() {
                let node = &(*node_ptr.as_ptr());
                match key.partial_cmp(&node.key) {
                    Some(Ordering::Greater) => {
                        current = &node.right;
                    }
                    Some(Ordering::Less) => {
                        current = &node.left;
                    },
                    Some(Ordering::Equal) => {
                        break
                    }
                    _ => panic!("find_link compared {} against {}", &key, &node.key)
                }
            }
        }
        current
    }

    /// Get mutable location of where a key would exist in the tree,
    /// regardless of whether it exists.
    fn find_link_location(&mut self, key: &K) -> LinkLocation<K> {
        let mut parent: Link<K>;
        let mut current: LinkPtr<K>;
        let mut found: bool = false;
        unsafe {
            parent = None;
            current = LinkPtr::new_unchecked(&mut self.root);

            while let Some(node_ptr) = current.as_ref() {
                let node = &mut (*node_ptr.as_ptr());
                match key.partial_cmp(&node.key) {
                    Some(Ordering::Greater) => {
                        parent = *current.as_ptr();
                        current = LinkPtr::new_unchecked(&mut node.right);
                    }
                    Some(Ordering::Less) => {
                        parent = *current.as_ptr();
                        current = LinkPtr::new_unchecked(&mut node.left);
                    },
                    Some(Ordering::Equal) => {
                        found = true;
                        break
                    }
                    _ => panic!("find_link_location compared {} against {}", &key, &node.key)
                }
            }
        }
        match found {
            true => { LinkLocation::Some {parent, link_ptr: current} },
            false => { LinkLocation::None {parent, link_ptr: current} }
        }
    }


    /// Insert key-value pair
    pub fn insert(&mut self, key: K, order: Option<Order>) {
        let location = self.find_link_location(&key);

        let order = {
            if order.is_none() { Default::default() }
            else { order.unwrap() }
        };

        match location {
            LinkLocation::None { mut parent, link_ptr } => {
                unsafe {
                    // println!("\nInserting {}\n", &key);
                    let link = &mut *link_ptr.as_ptr();

                    let mut new_link: Link<K> = {
                        let mut order_list = OrderStack::new();
                        order_list.push_back(order);
                        Some(Node::new(key, order_list, parent))
                    };

                    std::mem::swap(link, &mut new_link);
                    // self.display();
                    self.balance_stack(&mut parent);
                    self.len += 1;
                }
            },
            LinkLocation::Some {parent, link_ptr} => {
                // println!("Key {} already exists", &key);
                unsafe {
                    let node = &mut (*(*link_ptr.as_ptr()).unwrap().as_ptr());
                    node.value.push_back(order);
                }
            }
        }
    }

    /// Iteratively balances the tree starting from the passed link down to the root
    unsafe fn balance_stack(&mut self, link: &mut Link<K>) {
        // println!("balancing_stack from {} down to root. ", Self::debug_link(link));
        let mut current = link;
        while current.is_some() {
            self.balance(current);

            if Self::is_root(current) {
                break;
            } else {
                let parent = &mut (*current.unwrap().as_ptr()).parent;
                current = (self.flip_link(parent)).as_mut();
            }
        }
    }

    /// return true if link is root
    fn is_root(link: &Link<K>) -> bool {
        let node = unsafe { &(*link.unwrap().as_ptr()) };
        if node.parent.is_none() { true } else { false }
    }

    /// find whether a particular link is a root, left or right branch
    ///
    /// method will assume empty links are root, so be sure not to pass non-root empty links
    /// to this method
    fn get_parentage(link: &Link<K>) -> Branch {
        match link {
            None => Branch::Root,
            Some(node_ptr) => {
                let node = unsafe { &*(node_ptr.as_ptr()) };
                Node::get_parentage(node)
            }
        }
    }

    /// Get link's distance from root
    pub fn distance(link: &Link<K>) -> isize {
        let mut distance: isize = 0;
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
    pub fn height(link: &Link<K>) -> isize {
        if link.is_none() {return 0};
        let mut right_height: isize = 0;
        let mut left_height: isize = 0;
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

    /// Get link's balance factor by subtracting the link's right child
    /// height from its left child height
    fn balance_factor(link: &Link<K>) -> isize {
        let node = unsafe { &(*link.unwrap().as_ptr()) };
        let right_height = Self::height(&node.right);
        let left_height = Self::height(&node.left);
        right_height - left_height
    }

    /// Calls the link's applicable rotation method, dependent on its balance factor.
    ///
    /// After balancing, calls balance on the link's parent.
    fn balance(&mut self, link: &mut Link<K>) {
        if link.is_some() {
            let balance_factor = Self::balance_factor(link);
            let node = unsafe { &mut (*link.unwrap().as_ptr()) };

            // println!("Checking balance of {}...", Self::debug_link(link));
            if balance_factor > 1 {
                // println!();
                let balance_factor_right = Self::balance_factor(&node.right);
                if balance_factor_right < 0 {
                    // println!("Rotating for RL Case");
                    unsafe { self.rotate(link, LinkRotation::RLCase) };
                } else {
                    // println!("Rotating for RR Case");
                    unsafe { self.rotate(link, LinkRotation::RRCase) };
                };
            } else if balance_factor < -1 {
                // println!();
                let balance_factor_left = Self::balance_factor(&node.left);
                if balance_factor_left > 0 {
                    // println!("Rotating for LR Case");
                    unsafe { self.rotate(link, LinkRotation::LRCase) };
                } else {
                    // println!("Rotating for LL Case");
                    unsafe { self.rotate(link, LinkRotation::LLCase) };
                }
            } else {
                // println!("node is balanced.");
            }
        }
    }

    /// Method to handle node rotations for all cases. Returns mutable reference
    /// to new link that takes the passed link's place post-rotation.
    ///
    /// Returns a pointer to the top-most affected link post-rotation
    ///
    /// Reference:
    ///
    /// https://en.wikipedia.org/wiki/File:Tree_Rebalancing.gif"""
    unsafe fn rotate(&mut self, root: &mut Link<K>, case: LinkRotation) -> LinkPtr<K>{
        match case {
            LinkRotation::RRCase | LinkRotation::LLCase => {

                // Single rotations
                // for RR case, pivot's left child becomes root's right child
                // for LL case, pivot's right child becomes root's left child
                //
                // Affected nodes:
                // (1) parent - (a) child
                // (2) root - (a) parent, (b) left OR right child (left in LL, right in RR)
                // (3) pivot - (a) parent, (b) right OR left child (right in LL, left in RR)
                // (4) pivot's child (right in RR, left in LL) - (a) parent

                if case == LinkRotation::RRCase {
                    // println!("\nStart RR Case\n");
                    let pivot = &mut (*root.unwrap().as_ptr()).right;
                    let pivot_left = &mut (*pivot.unwrap().as_ptr()).left;

                    let parent = &mut (*root.unwrap().as_ptr()).parent;
                    let parent = &mut (*self.flip_link(parent).as_ptr());

                    let branch = Node::get_parentage(&(*root.unwrap().as_ptr()));
                    // println!("Root's branch = {:?}", branch);

                    // need pointers that persist through rotations
                    let parent_ptr = parent as *mut Link<K>;
                    let root_ptr = root as *mut Link<K>;
                    let pivot_ptr = pivot as *mut Link<K>;
                    let pivot_left_ptr = pivot_left as *mut Link<K>;

                    // println!("parent_ptr: {}\t\tparent: {}", Self::debug_link(&*parent_ptr), Self::debug_link(&parent));
                    // println!("root_ptr: {}\troot: {}", Self::debug_link(&*root_ptr), Self::debug_link(root));
                    // println!("pivot_ptr: {}\t\tpivot: {}", Self::debug_link(&*pivot_ptr), Self::debug_link(pivot));
                    // println!("pivot_left_ptr: {}\t\tpivot_left: {}", Self::debug_link(&*pivot_left_ptr), Self::debug_link(pivot_left));

                    // separate root from parent
                    let mut root: Link<K> = match branch {
                        Branch::Root => { self.root.take() },
                        Branch::Left => { (*parent.unwrap().as_ptr()).left.take() },
                        Branch::Right => { (*parent.unwrap().as_ptr()).right.take() },
                    };
                    // update root_ptr to maintain reference to root (now separated)
                    let root_ptr = &mut root as *mut Link<K>;

                    // separate pivot from root
                    let mut pivot: Link<K> = (*root.unwrap().as_ptr()).right.take();
                    // update pivot_ptr to maintain reference to pivot (now separated)
                    let pivot_ptr = &mut pivot as *mut Link<K>;

                    // separate pivot's child from pivot
                    let mut pivot_left: Link<K> = (*pivot.unwrap().as_ptr()).left.take();
                    // update pivot_left_ptr to maintain reference to pivot_left (now separated)
                    let pivot_left_ptr = &mut pivot_left as *mut Link<K>;

                    // update parent's child reference to pivot
                    match branch {
                        Branch::Root => { self.root = pivot },
                        Branch::Left => { (*parent.unwrap().as_ptr()).left = pivot },
                        Branch::Right => { (*parent.unwrap().as_ptr()).right = pivot },
                    }

                    // if root was tree's root, remove pivot's parent
                    // otherwise, update pivot's parent reference to parent
                    match branch {
                        Branch::Root => {
                            (*pivot.unwrap().as_ptr()).parent.take();
                        },
                        _ => {
                            (*pivot.unwrap().as_ptr()).parent = *parent_ptr;
                        }
                    }

                    // update pivot's left reference to root
                    (*pivot.unwrap().as_ptr()).left = *root_ptr;
                    // update root's parent reference to pivot
                    (*root.unwrap().as_ptr()).parent = *pivot_ptr;
                    // update root's right reference to pivot_left
                    (*root.unwrap().as_ptr()).right = *pivot_left_ptr;

                    // if pivot_left exists, update its parent reference to root
                    if pivot_left.is_some() {
                        (*pivot_left.unwrap().as_ptr()).parent = *root_ptr;
                    }

                    // println!("\n<<SWAPS>>\n");
                    // println!("parent_ptr: {}\t\tparent: {}", Self::debug_link(&*parent_ptr), Self::debug_link(&parent));
                    // println!("pivot_ptr: {}\t\tpivot: {}", Self::debug_link(&*pivot_ptr), Self::debug_link(&pivot));
                    // println!("root_ptr: {}\t\t\troot: {}", Self::debug_link(&*root_ptr), Self::debug_link(&root));
                    // println!("pivot_left_ptr: {}\t\tpivot_left: {}", Self::debug_link(&*pivot_left_ptr), Self::debug_link(&pivot_left));
                    //
                    // println!("\n RR Case End \n");

                    // self.display();

                    drop(parent_ptr);
                    drop(root_ptr);
                    drop(pivot_ptr);
                    drop(pivot_left_ptr);

                    NonNull::new_unchecked(parent_ptr)

                } else {  // LinkRotation::LLCase
                    assert!(case == LinkRotation::LLCase);
                    // println!("\nStart LL Case\n");

                    let pivot = &mut (*root.unwrap().as_ptr()).left;
                    let pivot_right = &mut (*pivot.unwrap().as_ptr()).right;
                    let parent = &mut (*root.unwrap().as_ptr()).parent;
                    let parent = &mut (*self.flip_link(parent).as_ptr());

                    let branch = Node::get_parentage(&(*root.unwrap().as_ptr()));
                    // println!("Root's branch = {:?}", branch);

                    // need pointers that persist through rotations
                    let parent_ptr = parent as *mut Link<K>;
                    let root_ptr = root as *mut Link<K>;
                    let pivot_ptr = pivot as *mut Link<K>;
                    let pivot_right_ptr = pivot_right as *mut Link<K>;

                    // println!("parent_ptr: {}\t\tparent: {}", Self::debug_link(&*parent_ptr), Self::debug_link(&parent));
                    // println!("root_ptr: {}\troot: {}", Self::debug_link(&*root_ptr), Self::debug_link(root));
                    // println!("pivot_ptr: {}\t\tpivot: {}", Self::debug_link(&*pivot_ptr), Self::debug_link(pivot));
                    // println!("pivot_right_ptr: {}\t\tpivot_right: {}", Self::debug_link(&*pivot_right_ptr), Self::debug_link(pivot_right));

                    // separate root from parent
                    let mut root: Link<K> = match branch {
                        Branch::Root => { self.root.take() },
                        Branch::Left => { (*parent.unwrap().as_ptr()).left.take() },
                        Branch::Right => { (*parent.unwrap().as_ptr()).right.take() },
                    };
                    // update root_ptr to maintain reference to root (now separated)
                    let root_ptr = &mut root as *mut Link<K>;

                    // separate pivot from root
                    let mut pivot: Link<K> = (*root.unwrap().as_ptr()).left.take();
                    // update pivot_ptr to maintain reference to pivot (now separated)
                    let pivot_ptr = &mut pivot as *mut Link<K>;

                    // separate pivot's child from pivot
                    let mut pivot_right: Link<K> = (*pivot.unwrap().as_ptr()).right.take();
                    // update pivot_right_ptr to maintain reference to pivot_right (now separated)
                    let pivot_right_ptr = &mut pivot_right as *mut Link<K>;

                    // update parent's child reference to pivot
                    match branch {
                        Branch::Root => { self.root = pivot },
                        Branch::Left => { (*parent.unwrap().as_ptr()).left = pivot },
                        Branch::Right => { (*parent.unwrap().as_ptr()).right = pivot },
                    }

                    // if root was tree's root, remove pivot's parent
                    // otherwise, update pivot's parent reference to parent
                    match branch {
                        Branch::Root => {
                            (*pivot.unwrap().as_ptr()).parent.take();
                        },
                        _ => {
                            (*pivot.unwrap().as_ptr()).parent = *parent_ptr;
                        }
                    }

                    // update pivot's right reference to root
                    (*pivot.unwrap().as_ptr()).right = *root_ptr;
                    // update root's parent reference to pivot
                    (*root.unwrap().as_ptr()).parent = *pivot_ptr;
                    // update root's left reference to pivot_right
                    (*root.unwrap().as_ptr()).left = *pivot_right_ptr;

                    // if pivot_right exists, update its parent reference to root
                    if pivot_right.is_some() {
                        (*pivot_right.unwrap().as_ptr()).parent = *root_ptr;
                    }

                    // println!("\n<<SWAPS DONE>>\n");
                    // println!("parent_ptr: {}\t\tparent: {}", Self::debug_link(&*parent_ptr), Self::debug_link(&parent));
                    // println!("pivot_ptr: {}\t\tpivot: {}", Self::debug_link(&*pivot_ptr), Self::debug_link(&pivot));
                    // println!("root_ptr: {}\t\t\troot: {}", Self::debug_link(&*root_ptr), Self::debug_link(&root));
                    // println!("pivot_right_ptr: {}\t\tpivot_right: {}", Self::debug_link(&*pivot_right_ptr), Self::debug_link(&pivot_right));
                    //
                    // println!("\n LL Case End \n");

                    // self.display();

                    drop(parent_ptr);
                    drop(root_ptr);
                    drop(pivot_ptr);
                    drop(pivot_right_ptr);

                    NonNull::new_unchecked(parent_ptr)
                }
            },

            LinkRotation::RLCase | LinkRotation::LRCase => {

                // Double Rotations
                let pivot: &mut Link<K>;
                if case == LinkRotation::RLCase {
                    // println!("\nStart RL Case - consists of LL -> RR\n");

                    let root_right = &mut (*root.unwrap().as_ptr()).right;
                    let pivot = &mut (*root_right.unwrap().as_ptr()).left;
                    let parent = &mut (*root.unwrap().as_ptr()).parent;
                    let parent = &mut (*self.flip_link(parent).as_ptr());

                    // println!("parent: {}", Self::debug_link(&parent));
                    // println!("root: {}", Self::debug_link(&root));
                    // println!("root_right: {}", Self::debug_link(&root_right));
                    // println!("pivot: {}", Self::debug_link(pivot));

                    let mut top = self.rotate(root_right, LinkRotation::LLCase);
                    // parent of first rotation is root of second rotation
                    let root = top.as_mut();
                    let pivot = &mut (*root.unwrap().as_ptr()).right;
                    let pivot_right = &mut (*pivot.unwrap().as_ptr()).right;

                    // println!("\nBack in RL - Intermediate Node States before RR Case\n");
                    // println!("root: {}", Self::debug_link(&root));
                    // println!("pivot: {}", Self::debug_link(&pivot));
                    // println!("pivot_right: {}", Self::debug_link(&pivot_right));

                    let mut top = self.rotate(root, LinkRotation::RRCase);
                    let pivot = top.as_mut();
                    let pivot_left = &mut (*pivot.unwrap().as_ptr()).left;
                    let pivot_right = &mut (*pivot.unwrap().as_ptr()).right;

                    // println!("\nBack in RL - Final Node States\n");
                    // println!("pivot: {}", Self::debug_link(&pivot));
                    // println!("pivot_left: {}", Self::debug_link(&pivot_left));
                    // println!("pivot_right: {}", Self::debug_link(&pivot_right));
                    //
                    // println!("\n RL Case End \n");

                    top

                } else {  // LinkRotation::LRCase
                    assert!(case == LinkRotation::LRCase);
                    // println!("\nStart LR Case - consists of RR -> LL\n");

                    let root_left = &mut (*root.unwrap().as_ptr()).left;
                    let pivot = &mut (*root_left.unwrap().as_ptr()).right;
                    let parent = &mut (*root.unwrap().as_ptr()).parent;
                    let parent = &mut (*self.flip_link(parent).as_ptr());

                    // println!("parent: {}", Self::debug_link(&parent));
                    // println!("root: {}", Self::debug_link(&root));
                    // println!("root_left: {}", Self::debug_link(&root_left));
                    // println!("pivot: {}", Self::debug_link(pivot));

                    let mut top = self.rotate(root_left, LinkRotation::RRCase);
                    // parent of first rotation is root of second rotation
                    let root = top.as_mut();
                    let pivot = &mut (*root.unwrap().as_ptr()).left;
                    let pivot_left = &mut (*pivot.unwrap().as_ptr()).left;

                    // println!("\nBack in LR - Intermediate Node States before LL Case\n");
                    // println!("root: {}", Self::debug_link(&root));
                    // println!("pivot: {}", Self::debug_link(&pivot));
                    // println!("pivot_left: {}", Self::debug_link(&pivot_left));

                    let mut top = self.rotate(root, LinkRotation::LLCase);
                    let pivot = top.as_mut();
                    let pivot_left = &mut (*pivot.unwrap().as_ptr()).left;
                    let pivot_right = &mut (*pivot.unwrap().as_ptr()).right;

                    // println!("\nBack in LR - Final Node States\n");
                    // println!("pivot: {}", Self::debug_link(&pivot));
                    // println!("pivot_left: {}", Self::debug_link(&pivot_left));
                    // println!("pivot_right: {}", Self::debug_link(&pivot_right));
                    //
                    // println!("\n LR Case End \n");

                    top
                }
            },
        }
    }

    /// Flip link from a child-parent reference to being a parent-child reference. Returns a
    /// link pointer.
    /// If the link passed to the method is empty, method will assume that it's the root node,
    /// which doesn't have a parent. Hence, need to take care not to call this method on
    /// empty links that are not the root node.
    fn flip_link(&mut self, link: &mut Link<K>) -> LinkPtr<K> {
        let link_ptr: LinkPtr<K>;
        let parent_branch = Self::get_parentage(&link);
        unsafe {
            match parent_branch {
                Branch::Root => {
                    link_ptr = NonNull::new_unchecked(&mut self.root as *mut Link<K>);
                },
                Branch::Left => {
                    let node = &mut (*link.unwrap().as_ptr());
                    let parent_node = &mut (*node.parent.unwrap().as_ptr());
                    link_ptr = NonNull::new_unchecked(&mut parent_node.left as *mut Link<K>);
                },
                Branch::Right => {
                    let node = &mut (*link.unwrap().as_ptr());
                    let parent_node = &mut (*node.parent.unwrap().as_ptr());
                    link_ptr = NonNull::new_unchecked(&mut parent_node.right as *mut Link<K>);
                },
            }
        }
        link_ptr
    }

    /// return stack of link references from passed link to root. primarily for debugging
    fn trace_parentage(link: &Link<K>) -> Vec<&Link<K>>{
        let mut stack: Vec<&Link<K>> = Vec::new();
        let mut current = link;
        loop {
            if current.is_some() {
                stack.push(current);
                current = unsafe { &(*current.unwrap().as_ptr()).parent };
            } else {
                break;
            }
        }
        stack
    }

    /// removes the passed link from the tree, replacing its position with the
    /// child denoted by the branch parameter.
    /// returns the removed link with parent, left and right pointers cleared
    /// only works with links with a single child.
    /// method will panic if link contains more than 1 child
    fn replace_with_child(&mut self, link: &mut Link<K>, which_child: Branch) -> Link<K> {
        let removed: Link<K>;
        unsafe {
            let node = &mut (*link.unwrap().as_ptr());

            // panic if link has more than 1 child
            match Node::which_children_exist(&node) {
                Children::Both => panic!(),
                _ => {}
            }

            let parent_branch = Self::get_parentage(&link);
            let parent: &mut Link<K> = &mut (*link.unwrap().as_ptr()).parent;

            let adopted_child: Link<K> = match which_child {
                Branch::Left => { node.left.take() },
                Branch::Right => { node.right.take() },
                _ => panic!(),
            };

            // debugging
            // println!("____replace with child____");
            // println!("link to be removed: {}", Self::debug_link(link));
            // println!("parent: {}", Self::debug_link(&parent));
            // println!("child that will be adopted: {}", Self::debug_link(&adopted_child));

            // pop link out of tree
            removed = std::mem::replace(link, adopted_child);
            // println!("------swapped------");

            // update child's parent pointer if it's not None
            if link.is_some() {
                match parent_branch {
                    Branch::Root => { (*link.unwrap().as_ptr()).parent = None },
                    _ => { (*link.unwrap().as_ptr()).parent = *parent }
                }
            }

            // debugging
            // println!("removed_link's old location: {}", Self::debug_link(link));
            // println!("removed link: {}", Self::debug_link(&removed));
            // println!("parent: {}", Self::debug_link(&parent));
            // println!("___________________________");
        }
        removed
    }

    /// Remove key-value pair from the tree, by key
    pub fn remove(&mut self, key: &K) -> Option<BoxedNode<K>> {
        let location = self.find_link_location(&key);
        self.remove_by_location(location)
    }

    /// Remove key-value pair from the treem, by link location
    fn remove_by_location(&mut self, location: LinkLocation<K>) -> Option<BoxedNode<K>> {
        // println!("\nCalled remove on {}", &key);

        let link_for_removal;
        let mut parent: Link<K>;
        let branch;

        match location {
            LinkLocation::None { parent: parent_link, link_ptr} => {
                return None
            },
            LinkLocation::Some { parent: parent_link, link_ptr} => {
                link_for_removal = unsafe { &mut (*link_ptr.as_ptr()) };
                parent = parent_link;
                branch = Self::get_parentage(link_for_removal);
            }
        }

        // to store final removed node
        let removed_node: BoxedNode<K>;

        let removed_link: Link<K>;
        unsafe {

            let mut successor_old_parent: &mut Link<K> = &mut None;

            match Node::which_children_exist(&(*link_for_removal.unwrap().as_ptr())) {
                Children::None => {
                    removed_link = link_for_removal.take();
                },

                Children::Left => {  // Parent inherits removed node's left child
                    removed_link = self.replace_with_child(link_for_removal, Branch::Left);
                },

                Children::Right => {  // Parent inherits removed node's right child
                    removed_link = self.replace_with_child(link_for_removal, Branch::Right)
                },

                // Choose a successor to replace removed node
                // Removed node's parent inherits successor
                // Successor either keeps its right child or inherits the removed node's right child
                // Successor inherits removed node's left child
                // Successor's parent inherits successor's right child
                // Successor has no left child to give away
                Children::Both => {

                    // need a raw pointer to work around borrow checker
                    let removed_node_ptr = (*link_for_removal)?.as_ptr();

                    // mutable ref to successor
                    let successor = Self::min_under_right(link_for_removal);

                    // temporary spot to hold successor in upcoming detachment / reattachment
                    let successor_temp: Link<K>;

                    // determine whether the successor's parent will adopt the successor's right child
                    // if successor is the removed node's right, it will remain in control of its right child
                    // otherwise, if the successor is somewhere further down the right subtree, it's right child
                    // will become the successor's parent's left child
                    let successor_key = Self::get_key(successor).unwrap();
                    let removed_node_right_key = Self::get_key(&(*removed_node_ptr).right).unwrap();
                    if successor_key != removed_node_right_key {
                        let removed_node_right = &mut (*removed_node_ptr).right;

                        // replace successor's old parent's left child with the successor node's right
                        successor_temp = self.replace_with_child(successor, Branch::Right);

                        // alias successor's node for readability and
                        // set successor's new right child and update the right child's parent reference
                        let successor_node = &mut (*successor_temp.unwrap().as_ptr());
                        (*successor_temp.unwrap().as_ptr()).right = *removed_node_right;
                        (*(*successor_temp.unwrap().as_ptr()).right.unwrap().as_ptr()).parent = successor_temp;

                    } else {
                        let removed_node_right = &mut (*removed_node_ptr).right;
                        successor_temp = removed_node_right.take();
                    }

                    // alias successor's node for readability
                    let successor_node = &mut (*successor_temp.unwrap().as_ptr());
                    // store mutable reference to successor's old parent for balancing starting point later
                    successor_old_parent = &mut (*self.flip_link(&mut successor_node.parent).as_ptr());

                    // give the successor the removed link's left child and update its parent reference
                    successor_node.left = (*removed_node_ptr).left.take();
                    (*successor_node.left.unwrap().as_ptr()).parent = successor_temp;

                    drop(removed_node_ptr);

                    // Replace removed link with the successor and update the successor's parent link
                    match branch {

                        Branch::Root => {
                            removed_link = std::mem::replace(&mut self.root, successor_temp);
                            (*self.root.unwrap().as_ptr()).parent.take();
                        },

                        Branch::Left => {
                            let parent_node_left = &mut (*parent.unwrap().as_ptr()).left;
                            removed_link = std::mem::replace(parent_node_left, successor_temp);
                            successor_node.parent = parent;
                        },

                        Branch::Right => {
                            let parent_node_right = &mut (*parent.unwrap().as_ptr()).right;
                            removed_link = std::mem::replace(parent_node_right, successor_temp);
                            successor_node.parent = parent;
                        },
                    }
                }
            }
            removed_node = Box::from_raw(removed_link.unwrap().as_ptr());
            // self.display();

            // If we just went through Children::Both removal, we need to begin balancing at the
            // successor's old parent
            if successor_old_parent.is_some() {
                self.balance_stack(&mut successor_old_parent);
            } else {
                self.balance_stack(&mut parent)
            }

        }
        self.len -= 1;
        Some(removed_node)
    }

    /// Return non-empty link with the smallest key that's greater than the passed link's key
    /// i.e. take one step right and then step left until the end
    /// Will return the same link that was passed if it has no right child or is empty
    fn min_under_right(link: &mut Link<K>) -> &mut Link<K> {
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
    fn max_under_left(link: &mut Link<K>) -> &mut Link<K> {
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
    fn get_key(link: &Link<K>) -> Option<K> {
        unsafe {
            Some((*(*link)?.as_ptr()).key.clone())
        }
    }

    /// Get key as string from link - DEBUGGING
    fn get_key_as_str(link: &Link<K>) -> String {
        let key_option = Self::get_key(link);
        match key_option {
            None => "None".to_string(),
            Some(key) => key.to_string(),
        }
    }
    
    /// Summarizes details about a link into a string
    fn debug_link(link: &Link<K>) -> String {
        match link {
            None => format!("EmptyLink"),
            Some(node_ptr) => {
                let mut details_vec: Vec<String> = Vec::new();
                let node = unsafe { &*node_ptr.as_ptr() };
                details_vec.push(format!("k:{}", &node.key));

                if let Some(parent_key) = Self::get_key(&node.parent) {
                    details_vec.push(format!("p:{}", parent_key))
                }
                if let Some(left_key) = Self::get_key(&node.left) {
                    details_vec.push(format!("l:{}", left_key))
                }
                if let Some(right_key) = Self::get_key(&node.right) {
                    details_vec.push(format!("r:{}", right_key))
                }

                details_vec.push(format!("h:{}", Self::height(&link)));
                details_vec.push(format!("b:{}", Self::balance_factor(&link)));

                format!("Link({})", details_vec.join("/"))
            }
        }
    }


    pub fn check_pointer_validity(&self, mut error_msgs: HashSet<String>) -> HashSet<String> {
        let mut tree_iter = self.iter();
        while let Some(node) = tree_iter.next() {
            if node.
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

    pub fn iter(&self) -> Iter<K> {
        Iter {
            current_link: self.root,
            first_move: true,
            len: self.len,
            _boo: PhantomData,
        }
    }
}

unsafe impl<K> Sync for AVLTree<K>
    where K: Sync + Display + Debug + PartialOrd + Clone {}

unsafe impl<K> Send for AVLTree<K>
    where K: Send + Display + Debug + PartialOrd + Clone {}

impl<'a, K> Iterator for Iter<'a, K>
    where K: Display + Debug + PartialOrd + Clone {
    type Item = (&'a K, &'a OrderStack);

    /// In-order BST traversal algorithm
    /// 1) For first move only, move down to the left-most link and return
    /// 2) If right subtree exists, move to the left-most link in it and return,
    ///         else, move up
    /// 3) If moving up to the right (i.e. from left child), return
    /// 4) If moving up to the left (i.e. from right child), continue
    /// 5) If parent is root, end traversal
    fn next(&mut self) -> Option<Self::Item> {
        let item;
        unsafe {
            match &self.first_move {

                true => {

                    if self.current_link.is_none() {
                        return None
                    }

                    // move down to the left-most link and return its item
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
                            let branch = AVLTree::get_parentage(&self.current_link);
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
                            }
                        }
                    }
                },
            }
        }
        item
    }
}

impl<'a, K> DoubleEndedIterator for Iter <'a, K>
    where K: Display + Debug + PartialOrd + Clone {

    /// In-order BST traversal algorithm
    /// 1) For first move only, move down to the right-most link and return
    /// 2) If left subtree exists, move to the right-most link in it and return,
    ///         else, move up
    /// 3) If moving up to the left (i.e. from right child), return
    /// 4) If moving up to the right (i.e. from left child), continue
    /// 5) If parent is root, end traversal
    fn next_back(&mut self) -> Option<Self::Item> {
        let item;
        unsafe {
            match &self.first_move {

                true => {

                    if self.current_link.is_none() {
                        return None
                    }

                    // move down to the right-most link and return its item
                    while (*self.current_link.unwrap().as_ptr()).right.is_some() {
                        self.current_link = (*self.current_link.unwrap().as_ptr()).right
                    }
                    self.first_move = false;
                    item = Some((
                        &(*self.current_link.unwrap().as_ptr()).key,
                        &(*self.current_link.unwrap().as_ptr()).value,
                    ));
                },

                false => {

                    // move down to the right-most link in the left-subtree
                    if (*self.current_link.unwrap().as_ptr()).left.is_some() {
                        self.current_link = (*self.current_link.unwrap().as_ptr()).left;

                        while (*self.current_link.unwrap().as_ptr()).right.is_some() {
                            self.current_link = (*self.current_link.unwrap().as_ptr()).right
                        }

                        // return key-value pair
                        item = Some((
                            &(*self.current_link.unwrap().as_ptr()).key,
                            &(*self.current_link.unwrap().as_ptr()).value,
                        ));

                    // otherwise, attempt to move up
                    } else {
                        loop {
                            let branch = AVLTree::get_parentage(&self.current_link);
                            match branch {
                                Branch::Root => {
                                    // if this is the root branch, then we have finished iterating over tree
                                    item = None;
                                    break;
                                }

                                Branch::Right => {
                                    // if this is the right branch, then we move up and to the left once
                                    self.current_link = (*self.current_link.unwrap().as_ptr()).parent;
                                    item = Some((
                                        &(*self.current_link.unwrap().as_ptr()).key,
                                        &(*self.current_link.unwrap().as_ptr()).value,
                                    ));
                                    break;
                                }

                                Branch::Left => {
                                    // if this is the left branch, then we continue
                                    // moving up until the branch is right or root
                                    self.current_link = (*self.current_link.unwrap().as_ptr()).parent;
                                }
                            }
                        }
                    }
                },
            }
        }
        item
    }
}

impl<K> Drop for AVLTree<K>
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

impl<K> Node<K>
    where K: Display + Debug + PartialOrd + Clone {
    /// Create new AVL Node
    fn new(key: K, value: OrderStack, parent: Link<K>) -> NodePtr<K> {
        let boxed_node = Box::new(Node {
            key,
            value,
            parent,
            left: None,
            right: None,
        });
        unsafe {
            NonNull::new_unchecked(Box::into_raw(boxed_node))
        }
    }

    /// Return which children a node has
    fn which_children_exist(&self) -> Children {
        match (&self.left, &self.right) {
            (None, None) => Children::None,
            (Some(_), None) => Children::Left,
            (None, Some(_)) => Children::Right,
            (Some(_), Some(_)) => Children::Both
        }
    }

    /// Get the node's key-value pair in a tuple
    pub fn items(&self) -> (&K, &OrderStack) {
        (&self.key, &self.value)
    }

    /// Get a copy of the node's key
    fn key(&self) -> K {
        (&self).key.clone()
    }

    /// Find whether self is left, right, or root branch of parent
    fn get_parentage(&self) -> Branch {
        if let Some(parent) = &self.parent {
            let parent_key = unsafe { &(*parent.as_ptr()).key };
            match (&self.key).partial_cmp(parent_key) {
                Some(Ordering::Greater) => Branch::Right,
                Some(Ordering::Less) => Branch::Left,
                _ => panic!("is_branch compared {} against {}", &self.key, parent_key),  // should never trigger
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
    fn display_aux(node: &Node<K>) -> (Vec<String>, usize, usize, usize) {
        // print!("display_aux - Node {} > ", &node.key);
        let children = Node::which_children_exist(&node);
        unsafe {
            match children {
                Children::None => {
                    // println!("no children >");
                    let line = format!("{:?}x{:?}/{:?}", &node.key, &node.value.cum_order_size().round(), &node.value.len());
                    let width = line.len();
                    let height = 1;
                    let middle = width / 2;
                    (vec![line], width, height, middle)
                }
                Children::Left => {
                    // println!("left child >");
                    let (lines, n, p, x) = Node::display_aux(&(*node.left.unwrap().as_ptr()));
                    // println!("back to {}", &node.key);
                    let s = format!("{:?}x{:?}/{:?}", &node.key, &node.value.cum_order_size().round(), &node.value.len());
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
                    let s = format!("{:?}x{:?}/{:?}", &node.key, &node.value.cum_order_size().round(), &node.value.len());
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
                    let s = format!("{:?}x{:?}/{:?}", &node.key, &node.value.cum_order_size().round(), &node.value.len());
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
    Right
}

enum Children {
    None,
    Left,
    Right,
    Both,
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
        let elements: Vec<T> = (0..amount)
            .map(|_| rng.gen_range(min..max))
            .filter(|x| !vector.contains(x))
            .collect();

        // println!("generating disjoint elements between {} and {}", min, max);
        elements
    }

    #[test]
    fn test_balancing() {
        println!("\n-----------TESTING TREE BALANCING-----------\n");
        let mut avl_tree: AVLTree<i32> = AVLTree::new();
        let mut rng = rand::thread_rng();
        // let mut keys: Vec<i32> = vec![71, 55, 62, 70, 88, 68, 12, 58, 43, 44, 75, 25, 27, 87, 91, 64, 97, 10, 72, 32];
        let mut keys: Vec<i32> = (0..20).map(|_| rng.gen_range(0..100)).collect();
        // Fill tree
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        keys.sort();
        keys.dedup();
        let mut expected_tree_length = keys.len();
        assert_eq!(avl_tree.len, expected_tree_length);

        let sample: Vec<i32> = keys.iter().map(|&k| k).choose_multiple(&mut rng, 5);
        // let sample = vec![75, 58, 88, 72, 70];
        let mut unsampled = vec![];
        for key in &keys {
            if !sample.contains(key) {
                unsampled.push(*key);
            }
        }

        println!("\nRemoving keys {:?}\n", &sample);
        println!("After removal, tree should contain {} keys {:?}\n", expected_tree_length - sample.len(), &unsampled);
        println!("Starting tree size = {}", avl_tree.len);
        for key in &sample {
            println!("Removing {}", &key);
            let removed = avl_tree.remove(&key);
            expected_tree_length -= 1;
            avl_tree.display();
            assert!(avl_tree.is_balanced());
            assert_eq!(avl_tree.len, expected_tree_length);
        }
        avl_tree.display();
        println!("Tree size = {}, expected = {}", avl_tree.len, expected_tree_length);
        println!("Remaining keys = {:?}", avl_tree.iter().map(|(k, v)| k).collect::<Vec<&i32>>());
    }

    #[test]
    fn test_insertion_and_link_finding() {
        println!("\n-----TESTING INSERTION AND LINK FINDING-----\n");
        let mut avl_tree: AVLTree<i32> = AVLTree::new();
        let mut rng = rand::thread_rng();
        // let keys: Vec<i32> = vec![29, 1, 9, 36, 48, 40, 46, 76, 79, 1, 94, 53, 29, 97, 83];
        let mut keys: Vec<i32> = (0..10).map(|_| rng.gen_range(0..100)).collect();
        // Fill tree
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        // Test presence of keys
        let sample: Vec<i32> = keys.iter().map(|x| *x).choose_multiple(&mut rng, 5);
        for key in &keys {
            let link = avl_tree.find_link(&key);
            assert!(link.is_some());
        }

        let disjoint: Vec<i32> = get_disjoint_elements(&mut rng, &sample, 5);
        println!("Testing disjoint elements: {:?}", disjoint);
        for key in disjoint {
            let link = avl_tree.find_link(&key);
            assert!(link.is_none());
        }
        println!();

        // Test tree size
        keys.sort();
        keys.dedup();
        assert_eq!(avl_tree.len, keys.len());
    }

    #[test]
    fn test_traversal() {
        println!("\n---------TESTING TREE TRAVERSAL---------\n");
        let mut avl_tree: AVLTree<f32> = AVLTree::new();
        let mut rng = rand::thread_rng();
        let mut keys: Vec<f32> = (0..20).map(|_| rng.gen_range(0..10000) as f32 / 100.0).collect();
        // let mut keys: Vec<f32> = vec![9.28, 7.58, 21.24, 0.15, 40.44, 47.91, 23.73, 74.31, 92.96, 94.17, 80.55, 88.54, 69.34, 85.36, 44.4, 19.64, 42.54, 5.14, 26.84, 3.27];
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        keys.sort_by(|a,b| a.partial_cmp(b).unwrap());
        keys.dedup();

        let inorder: Vec<f32> = avl_tree.iter().map(|x| *x.0).collect();
        let reverse: Vec<f32> = avl_tree.iter().rev().map(|x| *x.0).collect();
        let mut reverse_keys: Vec<f32> = keys.clone();
        reverse_keys.reverse();
        println!("In-order traversal (len {}): {:?}", inorder.len(), inorder);
        // println!("Sorted keys (len {}): {:?}", keys.len(), keys);
        for i in 0..keys.len() {
            assert_eq!(keys[i], inorder[i]);
            assert_eq!(reverse_keys[i], reverse[i]);
        }
    }

    #[test]
    fn test_removals() {
        println!("\n---------TESTING NODE REMOVALS---------\n");
        let mut avl_tree: AVLTree<f32> = AVLTree::new();
        let mut rng = rand::thread_rng();
        let mut keys: Vec<f32> = (0..10).map(|_| rng.gen_range(0..10000) as f32 / 100.0).collect();
        // let mut keys: Vec<f32> = vec![21.38, 14.79, 6.95, 26.43, 44.5, 79.57, 82.11, 20.5, 86.45, 67.8];
        println!("Filling tree with {:?}", keys);
        for key in &keys {
            avl_tree.insert(key.clone(), None);
        }
        avl_tree.display();
        keys.sort_by(|a,b| a.partial_cmp(b).unwrap());
        keys.dedup();

        let remove_keys = keys.iter().map(|&k| k).choose_multiple(&mut rng, 15);
        // let remove_keys = vec![6.95, 14.79, 20.50, 21.38, 26.43, 44.50, 67.80, 79.57, 82.11, 86.45];
        let remove_keys_str: Vec<String> = remove_keys.iter().map(|x| convert(*x,2)).collect();
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
