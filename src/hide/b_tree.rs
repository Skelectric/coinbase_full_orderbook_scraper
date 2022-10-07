#![allow(unused_imports)]

// standard
use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Iter;
use std::str::FromStr;
// external
use btree_slab::BTreeMap as BTreeSlab;
use rust_decimal::Decimal;
use rust_decimal_macros::*;
// internal
use crate::orderbook_dec::{OrderStack, Order, Side};

trait New { fn new() -> Self; }

pub struct OrderBTree ( BTreeMap<Decimal, OrderStack> );

impl OrderBTree {
    pub fn new() -> Self { OrderBTree (BTreeMap::new()) }

    pub fn insert(&mut self, order: Order) {
        let price = Decimal::from_str(&*order.price).unwrap();

        self.0.entry(price)
            .and_modify(|stack| stack.push_back(order.clone()))
            .or_insert(OrderStack::build(order.clone()));
    }

    pub fn remove(&mut self, key: String) {
        let price = Decimal::from_str(&*key).unwrap();
        self.0.remove(&price).unwrap();
    }

    pub fn get_mut(&mut self, key: String) {
        let price = Decimal::from_str(&*key).unwrap();
        self.0.get_mut(&price).unwrap();
    }

    pub fn iter(&self) -> Iter<Decimal, OrderStack> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, seq::SliceRandom};
    use crate::orderbook_dec::{Side, Order, OrderStack};
    use std::collections::BTreeMap;
    use btree_slab::BTreeMap as BTreeSlab;
    use rust_decimal::prelude::FromPrimitive;

    fn generate_random_orders(size: usize) -> Vec<Order> {
        let mut rng = rand::thread_rng();

        let prices: Vec<String> = (0..size)
            .into_iter()
            .map(|_k| rng.gen_range(0..10000) as f64 / 100.0)
            .map(|float| float.to_string())
            .collect();

        let sizes: Vec<String> = (0..size)
            .into_iter()
            .map(|_k| rng.gen_range(0..10000) as f64 / 100.0)
            .map(|float| float.to_string())
            .collect();

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

    #[test]
    fn basic() {
        println!("\n------Testing BTree Basics------\n");
        let mut order_map = OrderBTree::new();
        let orders = generate_random_orders(20);
        for order in &orders {
            order_map.insert(order.clone())
        }

    }


}