#![allow(dead_code)]
// #![allow(unused_variables)]
// #![allow(unused_mut)]
// #![allow(unused_imports)]
// #![allow(unused_assignments)]

// Standard Library
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::iter::{Peekable};
use cc_traits::{Collection, Len, PushBack};
// Crates
use serde::{Serialize, Deserialize};
use pyo3::prelude::*;
use chrono::Utc;
use crate::avl_tree;
use crate::avl_tree::New;
// Homebrew
use crate::avl_tree::{AVLTree, Node};

/// Struct representing the Limit orderbook of a single market
#[pyclass]
pub struct LimitOrderbook {
    bids: AVLTree<f64, OrderStack, Order>,
    asks: AVLTree<f64, OrderStack, Order>,
    order_map: HashMap<String, (Side, f64)>,
    len: usize,
    items_processed: usize,
    error_msgs: HashSet<String>,
    avl_tree_size_display_cutoff: usize,
    timestamp: String,
    outlier_factor: f64,
    bid_cutoff: f64,
    ask_cutoff: f64,
    outliers: usize,
}

/// OrderStack is a FIFO deque
pub struct OrderStack(VecDeque<Order>);

/// Struct representing a single limit order pre-list-insertion
#[pyclass]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Order {
    #[pyo3(get, set)]
    pub uid: String,
    pub side: Side,
    pub price: f64,
    #[pyo3(get, set)]
    pub size: f64,
    pub timestamp: String,
}

/// Enum for differentiating between bids and asks.
/// Embedded integer exists solely for PyO3 support.
#[pyclass]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Side {
    Bids,
    Asks,
}

enum SubmitRust {
    Insert { order: Order },
    Remove { uid: String },
    Update { uid: String, new_size: f64 },
}

#[pyclass]
#[derive(Clone, Debug)]
pub enum Submit {
    Insert,
    Remove,
    Update
}

#[pymethods]
impl LimitOrderbook {
    /// Create a new limit orderbook instance with two embedded AVL trees (for bids and asks).
    #[new]
    pub fn new() -> Self {
        LimitOrderbook {
            bids: AVLTree::new(),
            asks: AVLTree::new(),
            order_map: HashMap::new(),
            len: 0,
            items_processed: 0,
            error_msgs: HashSet::new(),
            avl_tree_size_display_cutoff: 1000,
            timestamp: Utc::now().format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string(),
            outlier_factor: 2.0,
            bid_cutoff: 0.0,
            ask_cutoff: 0.0,
            outliers: 0,
        }
    }

    #[getter(items_processed)]
    /// Returns the count of items processed by the orderbook
    pub fn items_processed(&self) -> usize { self.items_processed }

    #[getter(outliers)]
    /// Returns the count of items ignored by the orderbook due to being an outlier
    pub fn outliers(&self) -> usize { self.outliers }

    #[getter(timestamp)]
    /// Returns the timestamp of the latest order processed by the orderbook
    pub fn timestamp(&self) -> String { self.timestamp.clone() }

    #[getter(best_ask)]
    /// Return the lowest asking price in the book
    pub fn best_ask(&self) -> Option<f64> {
        Some(self.asks.iter().next()?.key.clone())
    }

    #[getter(best_bid)]
    /// Return the highest bidding price in the book
    pub fn best_bid(&self) -> Option<f64> {
        Some(self.bids.iter().next_back()?.key.clone())
    }

    #[getter(node_count)]
    /// Return count of unique price levels
    pub fn node_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    #[getter(len)]
    /// Return count of outstanding orders in orderbook
    pub fn len(&self) -> usize {
        self.len
    }

    #[getter(error_msgs)]
    /// Return any error_msgs resulting from unsuccessful checks
    pub fn error_msgs(&self) -> HashSet<String> { self.error_msgs.clone() }

    /// Display AVL-trees for bids or asks
    pub fn display(&self, side: Side) {
        self.display_trees_aux(side, true);
    }

    /// Return true if order exists in tree
    pub fn has(&self, order_uid: String) -> bool {
        if let Some(_) = self.get_order(order_uid) { true } else { false }
    }

    /// Return vector of (f64, f64, f64) tuples representing current snapshot of price, marginal
    /// order size (aggregate order size at each level)
    /// and cumulative depth (integral of price * order size)
    fn levels(&self, side: Side) -> Vec<(f64, f64, f64)> {
        match side {
            Side::Bids => {
                self.bids.iter().rev().scan(0.0, |cumsum, node| Option::from({
                    *cumsum += node.key * node.value.size();
                    (node.key, node.value.size(), cumsum.clone())
                })).collect()
            },
            Side::Asks => {
                self.asks.iter().scan(0.0, |cumsum, node| Option::from({
                    *cumsum += node.key * node.value.size();
                    (node.key, node.value.size(), cumsum.clone())
                })).collect()
                // println!("rust ask levels \n {:?}", result);
                // result
            },
        }
    }

    /// Process a given order
    pub fn process(&mut self, order: Order, action: Submit) {
        let action = Self::parse_query(order, action);
        match action {
            Ok(SubmitRust::Insert { order }) => {
                self.insert(order);
            },
            Ok(SubmitRust::Remove { uid }) => {
                self.remove(uid);
                // Ok("Removed")
            },
            Ok(SubmitRust::Update { uid, new_size }) => {
                self.update(uid, new_size);
                // Ok("Updated")
            },
            Err(e) => {
                panic!("orderbook.process error on {}", e);
            }
        }
        self.items_processed += 1;
    }

    /// Return some notes regarding what has been processed so far
    pub fn log_notes(&self) -> String {
        let mut notes_vec: Vec<String> = Vec::new();

        if self.error_msgs.len() == 0 {
            notes_vec.push("No errors encountered".to_string());
        } else {
            notes_vec.push("******Errors encountered******".to_string());
            for msg in &self.error_msgs {
                notes_vec.push(msg.clone());
            }
            notes_vec.push("---------------------------------".to_string());
        }

        if self.bids.len() > self.avl_tree_size_display_cutoff {
            let mut msg = format!("Bids AVL Tree too large to display ({} nodes). ", self.bids.len());
            msg += &*format!("Increase avl_tree_size_display_cutoff ({}) \
                in orderbook to display larger trees", self.avl_tree_size_display_cutoff);
            notes_vec.push(msg);
        } else {
            let msg = format!("{} bid levels = {:?}", self.bids.len(), self.levels(Side::Bids));
            notes_vec.push(msg);
            notes_vec.extend(self.display_trees_aux(Side::Bids, false));
        }

        if self.asks.len() > self.avl_tree_size_display_cutoff {
            let mut msg = format!("Asks AVL Tree too large to display ({} nodes). ", self.asks.len());
            msg += &*format!("Increase avl_tree_size_display_cutoff ({}) \
                in orderbook to display larger trees", self.avl_tree_size_display_cutoff);
            notes_vec.push(msg);
        } else {
            let msg = format!("{} ask levels = {:?}", self.asks.len(), self.levels(Side::Asks));
            notes_vec.push(msg);
            notes_vec.extend(self.display_trees_aux(Side::Asks, false));
        }

        notes_vec.push(format!("Items processed by orderbook: {}", self.items_processed));
        notes_vec.push(format!("Outliers ignored by orderbook: {}", self.outliers));
        notes_vec.join("\n")
    }

    /// Perform checks
    pub fn check(&mut self) {
        let mut error_msgs: HashSet<String> = HashSet::new();
        error_msgs = self.bids.check(error_msgs);
        error_msgs = self.asks.check(error_msgs);

        if !self.bids.is_balanced() {
            error_msgs.insert("Bids are not balanced!".to_string());
        }
        if !self.asks.is_balanced() {
            error_msgs.insert("Asks are not balanced!".to_string());
        }
        self.error_msgs = error_msgs;
    }
}

impl LimitOrderbook {

    fn parse_query(order: Order, action: Submit) -> Result<SubmitRust, String> {
        match action {
            Submit::Insert => Ok(SubmitRust::Insert { order }),
            Submit::Remove => Ok(SubmitRust::Remove { uid: order.uid }),
            Submit::Update => Ok(SubmitRust::Update { uid: order.uid, new_size: order.size }),
        }
    }

    /// Get reference to an order in the limit orderbook by its order_uid
    pub fn get_order(&self, order_uid: String) -> Option<&Order> {
        if let Some((side, key)) = self.order_map.get(&*order_uid) {
            let order_stack = match side {
                Side::Bids => self.bids.get(key).unwrap(),
                Side::Asks => self.asks.get(key).unwrap(),
            };
            let order_ref = order_stack.get_order(order_uid).unwrap();
            Some(order_ref)
        } else {
            // println!("Order uid {} not found in order_map", order_uid);
            None
        }
    }

    /// Get mutable reference to an order in the limit orderbook by its order_uid
    fn get_order_mut(&mut self, order_uid: String) -> Option<&mut Order> {
        if let Some((side, key)) = self.order_map.get(&*order_uid) {
            let order_stack = match side {
                Side::Bids => self.bids.get_mut(key).unwrap(),
                Side::Asks => self.asks.get_mut(key).unwrap(),
            };
            let order_ref = order_stack.get_order_mut(order_uid).unwrap();
            Some(order_ref)
        } else {
            // println!("Order uid {} not found in order_map", order_uid);
            None
        }
    }

    /// Check if order meets outlier condition.
    /// If it doesn't, update bid/ask cutoffs.
    fn handle_outlier(&mut self, order: &Order) -> bool {
        // best_bid or best_ask being none means tree is empty, so no way to determine if
        // order has outlier price. Assume it isn't
        match order.side {
            Side::Bids => {
                if self.bids.is_empty() {
                    self.bid_cutoff = order.price / self.outlier_factor;
                    false
                } else if order.price > self.best_bid().unwrap() {
                    self.bid_cutoff = order.price / self.outlier_factor;
                    false
                } else if order.price > self.bid_cutoff {
                    false
                } else {
                    true
                }
            },
            Side::Asks => {
                if self.asks.is_empty() {
                    self.ask_cutoff = order.price * self.outlier_factor;
                    false
                } else if order.price < self.best_ask().unwrap() {
                    self.ask_cutoff = order.price * self.outlier_factor;
                    false
                } else if order.price < self.ask_cutoff {
                    false
                } else {
                    true
                }
            }
        }
    }

    /// Inserts an order. Returns true if inserted
    fn insert(&mut self, order: Order) -> bool {
        if !self.handle_outlier(&order) {
            match order.side {
                Side::Bids => {
                    self.bids.insert(order.price.clone(), Some(order.clone()));
                },
                Side::Asks => self.asks.insert(order.price.clone(), Some(order.clone())),
            };
            self.order_map.insert(order.uid, (order.side, order.price));
            self.len += 1;
            true
        } else {
            self.outliers += 1;
            false
        }
    }

    /// Removes an order
    fn remove(&mut self, order_uid: String) {
        if let Some((side, key)) = self.order_map.get(&*order_uid) {
            match side {
                Side::Bids => {
                    let order_stack = self.bids.get_mut(key).unwrap();
                    order_stack.remove(order_uid.clone());
                    if order_stack.is_empty() { self.bids.remove(key); } // todo: make a method to remove nodes by reference
                },
                Side::Asks => {
                    let order_stack = self.asks.get_mut(key).unwrap();
                    order_stack.remove(order_uid.clone());
                    if order_stack.is_empty() { self.asks.remove(key); }
                }
            }
            self.len -= 1;
            self.order_map.remove(&*order_uid);
        }
    }

    /// Updates an order
    fn update(&mut self, order_uid: String, new_size: f64) {
        if let Some(order) = self.get_order_mut(order_uid.clone()) {
            if new_size == 0.0 {
                self.remove(order_uid)
            } else {
                order.size = new_size;
            };
        }
    }

    /// Auxiliary method for displaying trees
    fn display_trees_aux(&self, side: Side, print: bool) -> Vec<String> {
        let mut tree_vector: Vec<String> = Vec::new();
        match side {
            Side::Bids => {
                tree_vector.push("\nBids: ".to_string());
                tree_vector.extend(self.bids.display())
            },
            Side::Asks => {
                tree_vector.push("\nAsks: ".to_string());
                tree_vector.extend(self.asks.display())
            }
        }
        if print {
            for line in &tree_vector {
                println!("{}", line);
            }
        }
        tree_vector
    }

    /// Iterate over every order in the orderbook.
    ///
    /// Does this by iterating over orders in each OrderStack, choosing OrderStacks
    /// using In-order traversal on first the bid tree and then the ask tree.
    fn iter(&self) -> Iter {
        let mut iter = Iter {
            side: Side::Bids,
            current_node: None,
            bid_tree_iter: self.bids.iter().peekable(),
            ask_tree_iter: self.asks.iter().peekable(),
            stack_iter: None,
        };

        if iter.bid_tree_iter.peek().is_some() {
            iter.current_node = iter.bid_tree_iter.next();
        } else if iter.ask_tree_iter.peek().is_some() {
            iter.current_node = iter.ask_tree_iter.next();
        };

        if let Some(node) = iter.current_node {
            let order_stack = &node.value;
            iter.stack_iter = Some(order_stack.0.iter());
        }

        iter
    }
}

pub struct Iter<'a> {
    side: Side,
    // current_node: Option<(&'a f64, &'a OrderStack)>,
    current_node: Option<&'a Node<f64, OrderStack>>,
    bid_tree_iter: Peekable<avl_tree::Iter<'a, f64, OrderStack, Order>>,
    ask_tree_iter: Peekable<avl_tree::Iter<'a, f64, OrderStack, Order>>,
    stack_iter: Option<std::collections::vec_deque::Iter<'a, Order>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Order;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result;
        loop {
            match &self.side {
                Side::Bids => {
                    if let Some(_node) = self.current_node {
                        // let key = &node.key;
                        // let order_stack = &node.value;
                        // println!("\ncurrent bid node is {}, with stack length {}", key, stack.len());
                        result = self.stack_iter.as_mut().unwrap().next();
                        if result.is_some() {
                            // println!("result = {:?}", result);
                            break
                        } else {
                            // println!("empty result. iterating to next node");
                            self.current_node = self.bid_tree_iter.next();
                            self.stack_iter = match self.current_node {
                                Some(node) => {
                                    Some(node.value.0.iter())
                                },
                                None => None,
                            };
                            continue
                        }
                    } else {
                        // println!("current_node is None. Moving to asks tree...");
                        self.side = Side::Asks;
                        self.current_node = self.ask_tree_iter.next();
                        if let Some(node) = self.current_node {
                            self.stack_iter = Some(node.value.0.iter());
                        }
                    }
                }

                Side::Asks => {
                    if let Some(_node) = self.current_node {
                        // let key = &node.key;
                        // let order_stack = &node.value;
                        // println!("\ncurrent ask node is {}, with stack length {}", key, stack.len());
                        // println!("iterating through stack_iter");
                        result = self.stack_iter.as_mut().unwrap().next();
                        if result.is_some() {
                            // println!("result = {:?}", result);
                            break
                        } else {
                            // println!("empty result. iterating to next node");
                            self.current_node = self.ask_tree_iter.next();
                            self.stack_iter = match self.current_node {
                                Some(node) => {
                                    Some(node.value.0.iter())
                                },
                                None => None,
                            };
                            continue
                        }
                    } else {
                        // println!("current_node is None. Iteration complete.");
                        result = None;
                        break
                    }
                }
            }
        }
        result
    }
}

impl OrderStack {
    /// Create new order stack instance
    pub fn new() -> Self {
        OrderStack( VecDeque::new() )
    }

    /// Return true if orderstack is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return immutable reference to an order by its order uid
    pub fn get_order(&self, order_uid: String) -> Option<&Order> {
        self.0.iter().find(|order| order.uid == order_uid)
    }

    /// Return mutable reference to an order by its order uid
    pub fn get_order_mut(&mut self, order_uid: String) -> Option<&mut Order> {
        self.0.iter_mut().find(|order| order.uid == order_uid)
    }

    /// Push order to the back of the stack
    pub fn push_back(&mut self, order: Order) {
        self.0.push_back(order);
    }

    /// Pop order from the front of the stack
    pub fn pop_front(&mut self) -> Option<Order> {
        self.0.pop_front()
    }

    /// Remove order from the stack, by uid
    pub fn remove(&mut self, order_uid: String) -> Option<Order> {
        let index = self.0.iter().position(|order| order.uid == order_uid)?;
        self.0.remove(index)
    }

    /// Return cumulative order size
    pub fn size(&self) -> f64 {
        self.0.iter().fold(0.0, |sum, order| sum + order.size)
    }

    /// Return order stack's size
    pub fn len(&self) -> usize { self.0.len() }

}

impl Collection for OrderStack { type Item = Order; }

impl PushBack for OrderStack {
    type Output = ();
    fn push_back(&mut self, element: Self::Item) -> Self::Output {
        type Item = Order;
        type Output = ();
        self.push_back(element)
    }
}

impl New for OrderStack {
    fn new() -> Self { OrderStack::new() }
}

impl Len for OrderStack {
    fn len(&self) -> usize { self.len() }
}

#[pymethods]
impl Order {
    /// Create order struct out of limit order parameters
    #[new]
    pub fn new(uid: String, side: Option<Side>, price: Option<f64>,
                size: Option<f64>, timestamp: String) -> Order {
        Order {
            uid,
            side: side.unwrap_or(Default::default()),
            price: price.unwrap_or(0.0),
            size: size.unwrap_or(0.0),
            timestamp
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!(
            "Order(uid={}, side={:?}, price={}, size={}, timestamp={})",
            self.uid, self.side, self.price, self.size, self.timestamp
        ))
    }
}

impl Default for Side {
    fn default() -> Self {
        Side::Bids
    }
}

impl Default for Order {
    fn default() -> Self {
        Order {
            uid: "default_uid".to_string(),
            side: Default::default(),
            price: 0.0,
            size: 0.0,
            timestamp: "default timestamp".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, seq::SliceRandom};
    use super::*;

    fn generate_random_orders(size: usize) -> Vec<Order> {
        let mut rng = rand::thread_rng();

        let prices: Vec<f64> = (0..size)
            .into_iter().map(|_k| rng.gen_range(0..10000) as f64 / 100.0).collect();
        let sizes: Vec<f64> = (0..size)
            .into_iter().map(|_k| rng.gen_range(0..10000) as f64 / 100.0).collect();

        let vector: Vec<Order>
            = (0..size).into_iter().map(|k| {
            Order::new(
                k.to_string(),
                if rng.gen_bool(0.5) { Some(Side::Bids) } else { Some(Side::Asks) },
                Some(prices.choose(&mut rng).unwrap().clone()),
                Some(sizes.choose(&mut rng).unwrap().clone()),
                "dummy_datetime".to_string(),
            )
        }).collect();
        vector
    }

    fn deserialize_orders_from_json(json_orders: Vec<&str>) -> Vec<Order> {
        let deserialized_orders: Vec<Order> = json_orders
            .iter()
            .map(|json_order| serde_json::from_str(&json_order).unwrap())
            .collect();
        deserialized_orders
    }

    fn outlier_check(order: &Order, factor: f64, top_level: (Option<f64>, Option<f64>)) -> (bool, (Option<f64>, Option<f64>)) {
        match order.side {
            Side::Bids => {
                if let Some(best_bid) = top_level.0 {
                    if order.price < best_bid / factor {
                        (true, (Some(best_bid), top_level.1))
                    } else if order.price > best_bid {
                        (false, (Some(order.price), top_level.1))
                    } else {
                        (false, (Some(best_bid), top_level.1))
                    }
                } else {
                    (false, (Some(order.price), top_level.1))
                }
            },
            Side::Asks => {
                if let Some(best_ask) = top_level.1 {
                    if order.price > best_ask * factor {
                        (true, (top_level.0, Some(best_ask)))
                    } else if order.price < best_ask {
                        (false, (top_level.0, Some(order.price)))
                    } else {
                        (false, (top_level.0, Some(best_ask)))
                    }
                } else {
                    (false, (top_level.0, Some(order.price)))
                }
            },
        }
    }

    #[test]
    fn empty_book() {
        let mut lob = LimitOrderbook::new();
        let orders = generate_random_orders(2);
        assert_eq!(lob.best_ask(), None);
        assert_eq!(lob.best_bid(), None);
        assert_eq!(lob.len(), 0);
        assert_eq!(lob.node_count(), 0);
        assert_eq!(lob.levels(Side::Bids), vec![]);
        assert_eq!(lob.levels(Side::Asks), vec![]);
        // assert_eq!(lob.liquidity(Side::Bids), vec![]);
        // assert_eq!(lob.liquidity(Side::Asks), vec![]);
        assert_eq!(lob.items_processed(), 0);
        lob.check();
        assert_eq!(lob.error_msgs(), HashSet::new());
        lob.process(orders[0].clone(), Submit::Remove);
        lob.process(orders[1].clone(), Submit::Update);
        assert_eq!(lob.len(), 0);
        println!("Orderbook timestamp: {}", lob.timestamp);
    }

    #[test]
    fn basics() {
        println!("\n-----Testing LOB Order Insertion-----\n");
        let mut orderbook: LimitOrderbook = LimitOrderbook::new();


        let orders = generate_random_orders(20);
        println!("\nInserting randomly-generated orders into orderbook.\n");

        // let json_orders = vec![
        //     "{\"uid\":\"0\",\"side\":{\"Asks\":1},\"price\":76.28,\"size\":99.35,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"1\",\"side\":{\"Bids\":0},\"price\":95.55,\"size\":87.12,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"2\",\"side\":{\"Asks\":1},\"price\":53.88,\"size\":30.54,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"3\",\"side\":{\"Asks\":1},\"price\":76.28,\"size\":87.12,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"4\",\"side\":{\"Bids\":0},\"price\":57.0,\"size\":64.08,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"5\",\"side\":{\"Bids\":0},\"price\":44.12,\"size\":60.9,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"6\",\"side\":{\"Asks\":1},\"price\":94.42,\"size\":99.35,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"7\",\"side\":{\"Bids\":0},\"price\":98.79,\"size\":46.21,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"8\",\"side\":{\"Bids\":0},\"price\":7.73,\"size\":28.57,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"9\",\"side\":{\"Asks\":1},\"price\":57.0,\"size\":96.04,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"10\",\"side\":{\"Bids\":0},\"price\":44.12,\"size\":34.79,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"11\",\"side\":{\"Asks\":1},\"price\":47.36,\"size\":43.91,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"12\",\"side\":{\"Asks\":1},\"price\":47.36,\"size\":28.21,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"13\",\"side\":{\"Bids\":0},\"price\":16.8,\"size\":96.04,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"14\",\"side\":{\"Bids\":0},\"price\":7.73,\"size\":34.79,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"15\",\"side\":{\"Asks\":1},\"price\":7.73,\"size\":96.04,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"16\",\"side\":{\"Asks\":1},\"price\":7.73,\"size\":99.35,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"17\",\"side\":{\"Bids\":0},\"price\":60.76,\"size\":37.54,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"18\",\"side\":{\"Bids\":0},\"price\":57.0,\"size\":85.97,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"19\",\"side\":{\"Bids\":0},\"price\":47.36,\"size\":99.05,\"timestamp\":\"dummy_datetime\"}",
        // ];
        // let orders = deserialize_orders_from_json(json_orders);
        // println!("Deserializing orders and inserting into orderbook.");

        // reduce generated orders simulating orderbook's outlier-checking behavior
        let mut outlier_n_best = (false, (None, None));
        let mut orders_adj: Vec<Order> = Vec::new();
        for order in &orders {
            // let serialized_order = serde_json::to_string(&order).unwrap();
            // println!("{:?},", serialized_order);
            // println!("{:?}", order);
            let inserted = orderbook.insert(order.clone());
            outlier_n_best = outlier_check(&order, 2.0, outlier_n_best.1);
            assert_eq!(inserted, !outlier_n_best.0);
            if !outlier_n_best.0 {
                orders_adj.push(order.clone());
            }
        }
        let orders = orders_adj;

        assert_eq!(orderbook.len(), orders.len());
        orderbook.display_trees_aux(Side::Bids, true);
        println!("Best bid: {:?}", orderbook.best_bid());
        println!("Buy-side levels: {:?}", orderbook.levels(Side::Bids));
        // println!("Buy-side cumulative order depth: {:?}", orderbook.liquidity(Side::Bids));
        orderbook.display_trees_aux(Side::Asks, true);
        println!("Best ask: {:?}", orderbook.best_ask());
        println!("Sell-side levels {:?}", orderbook.levels(Side::Asks));
        // println!("Sell-side cumulative order depth: {:?}", orderbook.liquidity(Side::Asks));

        // test updates
        let mut rng = rand::thread_rng();
        let updated_order = orders.choose(&mut rng).unwrap();
        let new_size = 1000.0;
        println!("\n------------Updating order {} with new order size {}------------\n", updated_order.uid, new_size);
        orderbook.update(updated_order.uid.clone(), new_size);
        let updated_order = orderbook.get_order(updated_order.uid.clone()).unwrap().clone();
        println!("{:?}", updated_order);
        assert_eq!(updated_order.size, 1000.0);
        orderbook.display_trees_aux(Side::Bids, true);
        orderbook.display_trees_aux(Side::Asks, true);
        assert_eq!(orderbook.len(), orders.len());


        let deleted: Vec<&Order> = orders.choose_multiple(&mut rng, 5).collect();
        let deleted_uids: Vec<String> = deleted.iter().map(|order| order.uid.clone()).collect();
        println!("\nDeleting orders {:?}", deleted_uids);
        for i in 0..deleted.len() {
            let serialized_order = serde_json::to_string(&deleted[i]).unwrap();
            println!("{:?}", serialized_order);
            orderbook.remove(deleted_uids[i].clone());
        }

        // let delete_orders = vec![
        //     "{\"uid\":\"13\",\"side\":{\"Bids\":0},\"price\":16.8,\"size\":96.04,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"9\",\"side\":{\"Asks\":1},\"price\":57.0,\"size\":96.04,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"4\",\"side\":{\"Bids\":0},\"price\":57.0,\"size\":64.08,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"12\",\"side\":{\"Asks\":1},\"price\":47.36,\"size\":28.21,\"timestamp\":\"dummy_datetime\"}",
        //     "{\"uid\":\"0\",\"side\":{\"Asks\":1},\"price\":76.28,\"size\":99.35,\"timestamp\":\"dummy_datetime\"}",
        // ];
        // let deleted = deserialize_orders_from_json(delete_orders);
        // let deleted_uids: Vec<String> = deleted.iter().map(|order| order.uid.clone()).collect();
        // println!("\n-----------------Deleting orders {:?}-----------------\n", deleted_uids);
        // for order in &deleted {
        //     orderbook.remove(order.uid.clone());
        // }


        orderbook.display_trees_aux(Side::Bids, true);
        orderbook.display_trees_aux(Side::Asks, true);
        assert_eq!(orderbook.len(), orders.len() - deleted.len());

        // create vector of expected orders, accounting for updated order and deletions
        let remaining: Vec<&Order> = orders
            .iter()
            .filter(|&order| !deleted_uids.contains(&order.uid))
            .map(|order| {if *order.uid == updated_order.uid { &updated_order} else { order } })
            .collect();
        println!("\nExpected Remaining orders:\n");
        for order in &remaining {
            print!("{:?}", order);
            println!(" Orderbook has this? {}", orderbook.has(order.uid.clone()));
        }

        println!("\nRemaining orders in LOB:\n");
        let mut orderbook_iter = orderbook.iter();
        let mut order_vector: Vec<&Order> = Vec::new();
        let mut i: usize = 0;
        while let Some(order) = orderbook_iter.next() {
            println!("{:?}", order);
            order_vector.push(order);
            i += 1;
            if i > remaining.len() {
                break;
            }
        }

        println!("\nRemaining orders in LOB, sorted by uid:\n");
        order_vector.sort_by_key(|order| order.uid.clone().parse::<usize>().unwrap());
        let mut i: usize = 0;
        for order in &order_vector {
            println!("{:?}", order);
            assert_eq!(remaining[i], *order);
            i += 1;
        }

    }
}