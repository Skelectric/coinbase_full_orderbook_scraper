#![allow(unused_variables)]
#![allow(dead_code)]
// #![allow(unused_mut)]
#![allow(unused_imports)]
// #![allow(unused_assignments)]

use alloc::alloc;
// Standard Library
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::iter::{zip, Peekable};
// Crates
use pyo3::prelude::*;
// Homebrew
use crate::avl_tree_py::AVLTree;

type SideKey = (Side, f64);
type Depth = Vec<(f64, f64)>;

/// Struct representing the Limit orderbook of a single market
#[pyclass]
pub struct LimitOrderbook {
    bids: AVLTree<f64>,
    asks: AVLTree<f64>,
    order_map: HashMap<String, SideKey>,
    len: usize,
}

/// OrderStack is a FIFO deque
#[derive(Default)]
pub struct OrderStack(VecDeque<Order>);

/// Struct representing a single limit order pre-list-insertion
#[pyclass]
#[derive(Default, Debug, Clone, PartialEq)]
pub struct Order {
    pub uid: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub timestamp: String,
}

#[derive(FromPyObject)]
#[derive(Clone, Debug, PartialEq)]
pub enum Side {
    Bids(i32),
    Asks(i32),
}

#[derive(FromPyObject)]
enum Action {
    Insert { order: Order },
    Remove { uid: String },
    Update { uid: String, new_size: f64 },
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
        }
    }

    /// Return the lowest asking price in the book
    pub fn best_ask(&self) -> Option<f64> {
        Some(self.asks.iter().next()?.0.clone())
    }

    /// Return the highest bidding price in the book
    pub fn best_bid(&self) -> Option<f64> {
        Some(self.bids.iter().next_back()?.0.clone())
    }

    /// Return count of unique price levels
    pub fn node_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    /// Return count of outstanding orders in orderbook
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return two (f64, f64) vectors representing current snapshot of price vs liquidity
    /// for bids or asks
    fn get_liquidity(&self, side: Side) -> Depth {
        let liquidity: Depth = match side {
            Side::Bids(_) => {
                let prices = self.bids.iter().rev().map(|(&k, v)| k);
                let depth = self.bids.iter().rev()
                    .scan(0.0, |cumsum, (k, v)| {
                        *cumsum += k * v.cum_order_size();
                        Some(cumsum.clone())
                    });
                zip(prices, depth).collect()
            },
            Side::Asks(_) => {
                let prices = self.asks.iter().map(|(&k, v)| k);
                let depth = self.asks.iter()
                    .scan(0.0, |cumsum, (k, v)| {
                        *cumsum += k * v.cum_order_size();
                        Some(cumsum.clone())
                    });
                zip(prices, depth).collect()
            },
        };
        liquidity
    }

    /// Process a given order
    pub fn process(&mut self, order: Order, action: String) {
        let action = Self::parse_query(order, action);
        match action {
            Ok(Action::Insert { order }) => {
                self.insert(order);
            },
            Ok(Action::Remove { uid }) => {
                self.remove(uid);
                // Ok("Removed")
            },
            Ok(Action::Update { uid, new_size }) => {
                self.update(uid, new_size);
                // Ok("Updated")
            },
            Err(e) => {
                panic!("orderbook.process error on {}", e);
            }
        }
    }

    /// Print AVL trees for bids and asks
    pub fn display_trees(&self, side: Side) {
        match side {
            Side::Bids(_) => {
                println!("\nBids: ");
                self.bids.display();
            },
            Side::Asks(_) => {
                println!("\nAsks: ");
                self.asks.display();
            }
        }
    }
}

impl LimitOrderbook {

    fn parse_query(order: Order, action: String) -> Result<Action, String> {
        match action.to_lowercase().as_str() {
            "insert" | "add" | "append" => Ok(Action::Insert { order }),
            "remove" | "delete" => Ok(Action::Remove { uid: order.uid }),
            "update" | "change" => Ok(Action::Update { uid: order.uid, new_size: order.size }),
            _ => Err(format!("Invalid query action {}", action)),
        }
    }

    /// Get reference to an order in the limit orderbook by its order_uid
    pub fn get_order(&self, order_uid: String) -> Option<&Order> {
        if let Some((side, key)) = self.order_map.get(&*order_uid) {
            let order_stack = match side {
                Side::Bids(_) => self.bids.get(key).unwrap(),
                Side::Asks(_) => self.asks.get(key).unwrap(),
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
                Side::Bids(_) => self.bids.get_mut(key).unwrap(),
                Side::Asks(_) => self.asks.get_mut(key).unwrap(),
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
            Side::Bids(_) => self.bids.insert(key, Some(order)),
            Side::Asks(_) => self.asks.insert(key, Some(order)),
        }
        self.order_map.insert(order_uid, (side, key));
        self.len += 1;
    }

    /// Removes an order
    fn remove(&mut self, order_uid: String) {
        if let Some((side, key)) = self.order_map.get(&*order_uid) {
            match side {
                Side::Bids(_) => {
                    let order_stack = self.bids.get_mut(key).unwrap();
                    order_stack.remove(order_uid.clone());
                    if order_stack.is_empty() { self.bids.remove(key); } // todo: make a method to remove nodes by reference
                },
                Side::Asks(_) => {
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
        if let Some(order) = self.get_order_mut(order_uid) {
            order.size = new_size;
        }
    }

    /// Iterate over every order in the orderbook.
    ///
    /// Does this by iterating over orders in each OrderStack, choosing OrderStacks
    /// using In-order traversal on first the bid tree and then the ask tree.
    fn iter(&self) -> Iter {
        Iter {
            side: Side::Bids(0),
            peek_bids: self.bids.iter().peekable(),
            peek_asks: self.asks.iter().peekable(),
            stack_iter: {
                if let Some((key, order_stack)) = self.bids.iter().peekable().peek() {
                    Some(order_stack.0.iter().peekable())
                } else if let Some((key, order_stack)) = self.asks.iter().peekable().peek() {
                    Some(order_stack.0.iter().peekable())
                } else { None }
            },
        }
    }
}

pub struct Iter<'a> {
    side: Side,
    peek_bids: Peekable<crate::avl_tree_py::Iter<'a, f64>>,
    peek_asks: Peekable<crate::avl_tree_py::Iter<'a, f64>>,
    stack_iter: Option<Peekable<std::collections::vec_deque::Iter<'a, Order>>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Order;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = None;

        loop {
            if self.side == Side::Bids(0) {
                if let Some((key, order_stack)) = self.peek_bids.peek() {
                    if self.stack_iter.as_mut().unwrap().peek().is_some() {
                        result = self.stack_iter.as_mut().unwrap().next();
                        break
                    } else {
                        self.peek_bids.next();
                        if let Some(peeked_node) = self.peek_bids.peek() {
                            self.stack_iter = Some(peeked_node.1.0.iter().peekable());
                            result = self.stack_iter.as_mut().unwrap().next();
                            break
                        } else {
                            self.side = Side::Asks(1);
                            continue
                        }
                    }
                }
            }

            if self.side == Side::Asks(1) {
                if let Some((key, order_stack)) = self.peek_asks.peek() {
                    if self.stack_iter.as_mut().unwrap().peek().is_some() {
                        result = self.stack_iter.as_mut().unwrap().next();
                        break
                    } else {
                        self.peek_asks.next();
                        if let Some(peeked_node) = self.peek_asks.peek() {
                            self.stack_iter = Some(peeked_node.1.0.iter().peekable());
                            result = self.stack_iter.as_mut().unwrap().next();
                            break
                        } else {
                            result = None;
                            break
                        }
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

impl Order {
    /// Create order struct out of limit order parameters
    pub fn new(uid: String, side: Side, price: f64,
                size: f64, timestamp: String) -> Order {
        Order { uid, side, price, size, timestamp }
    }
}

impl Default for Side {
    fn default() -> Self {
        Side::Bids(0)
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
                if rng.gen_bool(0.5) { Side::Bids(0) } else { Side::Asks(1) },
                prices.choose(&mut rng).unwrap().clone(),
                sizes.choose(&mut rng).unwrap().clone(),
                "dummy_datetime".to_string(),
            )
        }).collect();
        vector

    }

    #[test]
    fn basics() {
        println!("\n-----Testing LOB Order Insertion-----\n");
        let mut orderbook: LimitOrderbook = LimitOrderbook::new();
        let orders = generate_random_orders(20);
        println!("Inserting randomly-generated orders into orderbook.");
        for order in &orders {
            println!("{:?}", &order);
            orderbook.insert(order.clone());
        }
        assert_eq!(orderbook.len(), orders.len());

        orderbook.display_trees(Side::Bids(0));
        println!("Best bid: {:?}", orderbook.best_bid());
        println!("Buy-side liquidity: {:?}", orderbook.get_liquidity(Side::Bids(0)));
        orderbook.display_trees(Side::Asks(1));
        println!("Best ask: {:?}", orderbook.best_ask());
        println!("Sell-side liquidity: {:?}", orderbook.get_liquidity(Side::Asks(1)));

        let mut rng = rand::thread_rng();
        let updated_order = orders.choose(&mut rng).unwrap();
        let order_uid = updated_order.uid.clone();
        let new_size = 1000.0;
        println!("\nUpdating order {} with new order size {}", order_uid, new_size);
        orderbook.update(order_uid.clone(), new_size);
        println!("{:?}", orderbook.get_order(order_uid).unwrap());
        orderbook.display_trees(Side::Bids(0));
        orderbook.display_trees(Side::Asks(1));
        assert_eq!(orderbook.len(), orders.len());

        let deleted: Vec<&Order> = orders.choose_multiple(&mut rng, 5).collect();
        let deleted_uids: Vec<String> = deleted.iter().map(|order| order.uid.clone()).collect();
        println!("\nDeleting orders {:?}", deleted_uids);
        for i in 0..deleted.len() {
            println!("{:?}", deleted[i]);
            orderbook.remove(deleted_uids[i].clone());
        }
        orderbook.display_trees(Side::Bids(0));
        orderbook.display_trees(Side::Asks(1));
        assert_eq!(orderbook.len(), orders.len() - deleted.len());

        let remaining: Vec<&Order> = orders
            .iter().filter(|&order| !deleted_uids.contains(&order.uid)).collect();
        println!("\nExpected Remaining orders:\n");
        for order in &remaining {
            println!("{:?}", order);
        }

        println!("\nRemaining orders in LOB:\n");
        let mut order_vector: Vec<&Order> = orderbook.iter().collect();
        order_vector.sort_by_key(|order| order.uid.clone().parse::<usize>().unwrap());
        let mut i: usize = 0;
        for order in order_vector {
            println!("{:?}", order);
            if order != updated_order {
                assert_eq!(remaining[i], order);
            }
            i += 1;
        }

    }
}