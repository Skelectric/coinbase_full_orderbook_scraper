#![allow(unused_variables)]
#![allow(dead_code)]
// #![allow(unused_mut)]
#![allow(unused_imports)]
// #![allow(unused_assignments)]

// Standard Library
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::iter::zip;
// Crates
use rust_decimal_macros::dec;
use rust_decimal::prelude::*;
use chrono::{DateTime, Utc};
// Homebrew
use crate::avl_tree::AVLTree;

type SideKey = (Side, Decimal);

/// Struct representing the Limit orderbook of a single market
pub struct LimitOrderbook {
    bids: AVLTree<Decimal>,
    asks: AVLTree<Decimal>,
    order_map: HashMap<String, SideKey>,
    // timestamp: DateTime<Utc>,
}

/// OrderStack is a FIFO deque
#[derive(Default)]
pub struct OrderStack(VecDeque<Order>);

/// Struct representing a single limit order pre-list-insertion
#[derive(Default, Debug, Clone)]
pub struct Order {
    pub uid: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Default, Debug)]
pub enum Side {
    #[default]
    Bids,
    Asks,
}

enum Action {
    Insert { order: Order },
    Remove { uid: String },
    Update { uid: String, new_size: Decimal },
}

type Depth = Vec<(Decimal, Decimal)>;

impl LimitOrderbook {

    /// Create a new limit orderbook instance with two embedded AVL trees (for bids and asks).
    pub fn new() -> Self {
        LimitOrderbook {
            bids: AVLTree::new(),
            asks: AVLTree::new(),
            order_map: HashMap::new(),
        }
    }

    /// Return the lowest asking price in the book
    pub fn best_ask(&self) -> Option<&Decimal> {
        Some(self.asks.iter().next()?.0)
    }

    /// Return the highest bidding price in the book
    pub fn best_bid(&self) -> Option<&Decimal> {
        Some(self.bids.iter().next_back()?.0)
    }

    /// Return two (Decimal, Decimal) vectors representing current snapshot of price vs liquidity
    /// for bids or asks
    fn get_liquidity(&self, side: Side) -> Depth {
        let liquidity: Depth = match side {
            Side::Bids => {
                let prices = self.bids.iter().rev().map(|(&k, v)| k);
                let depth = self.bids.iter().rev()
                    .scan(dec![0.0], |cumsum, (k, v)| {
                        *cumsum += k * v.cum_order_size();
                        Some(cumsum.clone())
                    });
                zip(prices, depth).collect()
            },
            Side::Asks => {
                let prices = self.asks.iter().map(|(&k, v)| k);
                let depth = self.asks.iter()
                    .scan(dec![0.0], |cumsum, (k, v)| {
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
    }

    /// Removes an order
    fn remove(&mut self, order_uid: String) {
        todo!()
    }

    /// Updates an order
    fn update(&mut self, order_uid: String, new_size: Decimal) {
        if let Some(order) = self.get_order_mut(order_uid) {
            order.size = new_size;
        }
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
}

impl OrderStack {
    /// Create new order stack instance
    pub fn new() -> Self {
        OrderStack( VecDeque::new() )
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

    /// Remove order from the stack, by index
    pub fn remove(&mut self, index: usize) -> Option<Order> {
        self.0.remove(index)
    }

    /// Return cumulative order size
    pub fn cum_order_size(&self) -> Decimal {
        self.0.iter().fold(dec![0], |sum, order| sum + order.size)
    }

    /// Return orderstack's size
    pub fn len(&self) -> usize {
        self.0.len()
    }

}

impl Order {
    /// Create order struct out of limit order parameters
    pub fn new(uid: String, side: Side, price: Decimal,
                size: Decimal, timestamp: DateTime<Utc>) -> Order {
        Order { uid, side, price, size, timestamp }
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, seq::SliceRandom};
    use super::*;

    fn generate_random_orders(size: usize) -> Vec<Order> {
        let mut rng = rand::thread_rng();

        let prices: Vec<f32> = (0..size)
            .into_iter().map(|k| rng.gen_range(0..10000) as f32 / 100.0).collect();
        let sizes: Vec<f32> = (0..size)
            .into_iter().map(|k| rng.gen_range(0..10000) as f32 / 100.0).collect();

        let vector: Vec<Order>
            = (0..size).into_iter().map(|k| {
            Order::new(
                k.to_string(),
                if rng.gen_bool(0.5) { Side::Bids } else { Side::Asks },
                Decimal::from_f32(prices.choose(&mut rng).unwrap().clone()).unwrap(),
                Decimal::from_f32(sizes.choose(&mut rng).unwrap().clone()).unwrap(),
                Utc::now(),
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
            println!("Inserting {:?}", &order);
            orderbook.insert(order.clone());
        }
        orderbook.display_trees(Side::Bids);
        println!("Best bid: {:?}", orderbook.best_bid());
        println!("Buy-side liquidity: {:?}", orderbook.get_liquidity(Side::Bids));
        orderbook.display_trees(Side::Asks);
        println!("Best ask: {:?}", orderbook.best_ask());
        println!("Sell-side liquidity: {:?}", orderbook.get_liquidity(Side::Asks));

        let mut rng = rand::thread_rng();
        let update_order = orders.choose(&mut rng).unwrap();
        let order_uid = update_order.uid.clone();
        let new_size = dec!(1000);
        println!("\nUpdating order {} with new order size {}", order_uid, new_size);
        orderbook.update(order_uid.clone(), new_size);
        println!("{:?}", orderbook.get_order(order_uid).unwrap());
        orderbook.display_trees(Side::Bids);
        orderbook.display_trees(Side::Asks);




    }
}