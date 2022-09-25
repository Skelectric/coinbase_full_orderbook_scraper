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

/// Struct representing the Limit orderbook of a single market
pub struct LimitOrderbook<T> {
    bids: AVLTree<Decimal, OrderStack>,
    asks: AVLTree<Decimal, OrderStack>,
    order_map: HashMap<String, T>,
    timestamp: DateTime<Utc>,
}

/// Struct representing a stack of limit orders at a single price level
struct OrderStack {
    orders: VecDeque<Order>,
    size: Decimal,
}

/// Struct representing an element of the order_map
pub struct Order {
    uid: String,
    side: Side,
    price: Decimal,
    size: Decimal,
    timestamp: DateTime<Utc>,
}

pub enum Side {
    Bids,
    Asks,
}

enum MarketDepthOption {
    Marginal,
    Cumulative,
}

enum Action {
    Insert { order: Order },
    Remove { uid: String },
    Update { uid: String, new_size: Decimal },
}

type Depth = Vec<(Decimal, Decimal)>;

impl<T> LimitOrderbook<T> {

    /// Create a new limit orderbook instance with two embedded AVL trees (for bids and asks).
    pub fn new() -> Self {
        LimitOrderbook {
            bids: AVLTree::new(),
            asks: AVLTree::new(),
            order_map: HashMap::new(),
            timestamp: Utc::now(),
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
    /// for both bids and asks.
    fn get_liquidity(&self, side: Side, option: MarketDepthOption) -> Depth {
        let liquidity: Depth = match side {
            Side::Bids => {
                match option {
                    MarketDepthOption::Marginal => {
                        self.bids
                            .iter()
                            .map(|(k, v)| (k.clone(), v.size.clone()))
                            .collect()
                    },
                    MarketDepthOption::Cumulative => {
                        let prices = self.bids.iter().map(|(&k, v)| k);
                        let depth = self.bids.iter()
                            .scan(dec![0.0], |cumsum, (k, v)| {
                                *cumsum += v.size;
                                Some(cumsum.clone())
                            });
                        zip(prices, depth).collect()
                    }
                }
            },

            Side::Asks => {
                match option {
                    MarketDepthOption::Marginal => {
                        self.asks
                            .iter()
                            .map(|(k, v)| (k.clone(), v.size.clone()))
                            .collect()
                    },
                    MarketDepthOption::Cumulative => {
                        let prices = self.asks.iter().map(|(&k, v)| k);
                        let depth = self.bids.iter()
                            .scan(dec![0.0], |cumsum, (k, v)| {
                                *cumsum += v.size;
                                Some(cumsum.clone())
                            });
                        zip(prices, depth).collect()
                    }
                }
            },

        };
        liquidity
    }

    /// Process a given order
    // todo: return Result type indicating processing status
    pub fn process(&mut self, order: Order, action: String) {
        let action = Self::parse_query(order, action);
        match action {
            Ok(Action::Insert { order }) => {
                self.insert(order);
                // Ok("Inserted")
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

    /// Inserts an order
    fn insert(&mut self, order: Order) {
        todo!()
    }

    /// Removes an order
    fn remove(&mut self, order_uid: String) {
        todo!()
    }

    /// Updates an order
    fn update(&mut self, order_uid: String, new_size: Decimal) {
        todo!()
    }
}

impl OrderStack {
    /// Create new order stack instance
    pub fn new() -> Self {
        OrderStack { orders: VecDeque::new(), size: dec![0.00] }
    }
}

impl Order {
    /// Create new order instance
    pub fn new(uid: String, side: Side, price: Decimal,
               size: Decimal, timestamp: DateTime<Utc>) -> Self {
        Order { uid, side, price, size, timestamp, }
    }
}

#[cfg(test)]
mod tests {
    use crate::avl_tree;

    #[test]
    fn basics() {

    }

}