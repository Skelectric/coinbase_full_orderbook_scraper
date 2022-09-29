#![allow(unused_variables)]
#![allow(dead_code)]
// #![allow(unused_mut)]
#![allow(unused_imports)]
// #![allow(unused_assignments)]

// Standard Library
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Display};
use std::iter::{zip, Peekable};
// Crates
use serde::{Serialize, Deserialize};
use pyo3::prelude::*;
// Homebrew
use crate::avl_tree_py::AVLTree;

type SideKey = (Side, f64);

/// Struct representing the Limit orderbook of a single market
#[pyclass]
pub struct LimitOrderbook {
    bids: AVLTree<f64>,
    asks: AVLTree<f64>,
    order_map: HashMap<String, SideKey>,
    len: usize,
    items_processed: usize,
    error_msgs: HashSet<String>,
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
        }
    }

    #[getter(items_processed)]
    /// Returns the count of items processed by the orderbook
    pub fn items_processed(&self) -> usize {
        self.items_processed
    }

    #[getter(best_ask)]
    /// Return the lowest asking price in the book
    pub fn best_ask(&self) -> Option<f64> {
        Some(self.asks.iter().next_back()?.0.clone())
    }

    #[getter(best_bid)]
    /// Return the highest bidding price in the book
    pub fn best_bid(&self) -> Option<f64> {
        Some(self.bids.iter().next_back()?.0.clone())
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

    /// Return vector of (f64, f64) tuples representing current snapshot of price vs marginal
    /// outstanding limit order size for bids OR asks.
    fn levels(&self, side: Side) -> Vec<(f64, f64)> {
        match side {
            Side::Bids => {
                self.bids.iter().rev().map(|(k, v)| (k.clone(), v.cum_order_size())).collect()
            },
            Side::Asks => {
                self.asks.iter().map(|(k, v)| (k.clone(), v.cum_order_size())).collect()
            },
        }
    }


    /// Return vector of (f64, f64) tuples representing current snapshot of price vs cumulative
    /// outstanding limit order size for bids OR asks.
    fn liquidity(&self, side: Side) -> Vec<(f64, f64)> {
        let liquidity: Vec<(f64, f64)> = match side {
            Side::Bids => {
                let prices = self.bids.iter().rev().map(|(&k, v)| k);
                let cumulative = self.bids.iter().rev()
                    .scan(0.0, |cumsum, (k, v)| {
                        *cumsum += k * v.cum_order_size();
                        Some(cumsum.clone())
                    });
                zip(prices, cumulative).collect()
            },
            Side::Asks => {
                let prices = self.asks.iter().map(|(&k, v)| k);
                let cumulative = self.asks.iter()
                    .scan(0.0, |cumsum, (k, v)| {
                        *cumsum += k * v.cum_order_size();
                        Some(cumsum.clone())
                    });
                zip(prices, cumulative).collect()
            },
        };
        liquidity
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
        self.items_processed += 1
    }

    /// Print AVL trees for bids and asks
    pub fn display_trees(&self, side: Side) {
        match side {
            Side::Bids => {
                println!("\nBids: ");
                self.bids.display();
            },
            Side::Asks => {
                println!("\nAsks: ");
                self.asks.display();
            }
        }
    }

    /// Return true if order exists in tree
    pub fn has(&self, order_uid: String) -> bool {
        if let Some(_) = self.get_order(order_uid) { true } else { false }
    }

    /// Log some details regarding what has been processed so far
    pub fn log_details(&self) {
        // todo
    }

    /// Perform checks
    pub fn check(&self) -> HashSet<String> {
        let mut error_msgs: HashSet<String> = HashSet::new();
        error_msgs = self.bids.check_pointer_validity(error_msgs);
        error_msgs = self.asks.check_pointer_validity(error_msgs);

        if !self.bids.is_balanced() {
            error_msgs.insert("Bids are not balanced!".to_string());
        }
        if !self.asks.is_balanced() {
            error_msgs.insert("Asks are not balanced!".to_string());
        }
        error_msgs
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
            println!("Order uid {} not found in order_map", order_uid);
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
            println!("Order uid {} not found in order_map", order_uid);
            None
        }
    }

    /// Inserts an order
    fn insert(&mut self, order: Order) {
        let order_uid = order.uid.clone();
        let side = order.side.clone();
        let key = order.price.clone();
        match order.side {
            Side::Bids => self.bids.insert(key, Some(order)),
            Side::Asks => self.asks.insert(key, Some(order)),
        }
        self.order_map.insert(order_uid, (side, key));
        self.len += 1;
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

        if let Some((key, order_stack)) = iter.current_node {
            iter.stack_iter = Some(order_stack.0.iter());
        }

        iter
    }
}

pub struct Iter<'a> {
    side: Side,
    current_node: Option<(&'a f64, &'a OrderStack)>,
    bid_tree_iter: Peekable<crate::avl_tree_py::Iter<'a, f64>>,
    ask_tree_iter: Peekable<crate::avl_tree_py::Iter<'a, f64>>,
    stack_iter: Option<std::collections::vec_deque::Iter<'a, Order>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Order;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result;
        loop {
            match &self.side {
                Side::Bids => {
                    if let Some((key, stack)) = self.current_node {
                        // println!("\ncurrent bid node is {}, with stack length {}", key, stack.len());
                        result = self.stack_iter.as_mut().unwrap().next();
                        if result.is_some() {
                            // println!("result = {:?}", result);
                            break
                        } else {
                            // println!("empty result. iterating to next node");
                            self.current_node = self.bid_tree_iter.next();
                            self.stack_iter = match self.current_node {
                                Some((key, order_stack)) => {
                                    Some(order_stack.0.iter())
                                },
                                None => None,
                            };
                            continue
                        }
                    } else {
                        // println!("current_node is None. Moving to asks tree...");
                        self.side = Side::Asks;
                        self.current_node = self.ask_tree_iter.next();
                        self.stack_iter = Some(self.current_node.unwrap().1.0.iter());
                    }
                }

                Side::Asks => {
                    if let Some((key, stack)) = self.current_node {
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
                                Some((key, order_stack)) => {
                                    Some(order_stack.0.iter())
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
    pub fn cum_order_size(&self) -> f64 {
        self.0.iter().fold(0.0, |sum, order| sum + order.size)
    }

    /// Return order stack's size
    pub fn len(&self) -> usize { self.0.len() }

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
            .into_iter().map(|k| rng.gen_range(0..10000) as f64 / 100.0).collect();
        let sizes: Vec<f64> = (0..size)
            .into_iter().map(|k| rng.gen_range(0..10000) as f64 / 100.0).collect();

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

    #[test]
    fn empty_book() {
        let lob = LimitOrderbook::new();
        assert_eq!(lob.best_ask(), None);
        assert_eq!(lob.best_bid(), None);
        assert_eq!(lob.len(), 0);
        assert_eq!(lob.node_count(), 0);
        assert_eq!(lob.levels(Side::Bids), vec![]);
        assert_eq!(lob.levels(Side::Asks), vec![]);
        assert_eq!(lob.liquidity(Side::Bids), vec![]);
        assert_eq!(lob.liquidity(Side::Asks), vec![]);
        assert_eq!(lob.items_processed(), 0);
    }

    #[test]
    fn basics() {
        println!("\n-----Testing LOB Order Insertion-----\n");
        let mut orderbook: LimitOrderbook = LimitOrderbook::new();


        let orders = generate_random_orders(20);
        println!("Inserting randomly-generated orders into orderbook.");

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

        for order in &orders {
            let serialized_order = serde_json::to_string(&order).unwrap();
            println!("{:?},", serialized_order);
            // println!("{:?}", order);
            orderbook.insert(order.clone());
        }
        assert_eq!(orderbook.len(), orders.len());
        orderbook.display_trees(Side::Bids);
        println!("Best bid: {:?}", orderbook.best_bid());
        println!("Buy-side marginal order depth: {:?}", orderbook.levels(Side::Bids));
        println!("Buy-side cumulative order depth: {:?}", orderbook.liquidity(Side::Bids));
        orderbook.display_trees(Side::Asks);
        println!("Best ask: {:?}", orderbook.best_ask());
        println!("Sell-side marginal order depth: {:?}", orderbook.levels(Side::Asks));
        println!("Sell-side cumulative order depth: {:?}", orderbook.liquidity(Side::Asks));

        // test updates
        let mut rng = rand::thread_rng();
        let updated_order = orders.choose(&mut rng).unwrap();
        let new_size = 1000.0;
        println!("\n------------Updating order {} with new order size {}------------\n", updated_order.uid, new_size);
        orderbook.update(updated_order.uid.clone(), new_size);
        let updated_order = orderbook.get_order(updated_order.uid.clone()).unwrap().clone();
        println!("{:?}", updated_order);
        assert_eq!(updated_order.size, 1000.0);
        orderbook.display_trees(Side::Bids);
        orderbook.display_trees(Side::Asks);
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


        orderbook.display_trees(Side::Bids);
        orderbook.display_trees(Side::Asks);
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