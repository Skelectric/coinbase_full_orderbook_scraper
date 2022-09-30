pub mod orderbook_avl_tree;
mod avl_tree;
mod orderbook_btree_slab;

use pyo3::prelude::*;
use orderbook_avl_tree::*;

/// An unsafe AVL-tree limit orderbook written in Rust
///
/// No UB and thread-safety not guaranteed
#[pymodule]
fn rust_orderbook(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<LimitOrderbook>()?;
    m.add_class::<Order>()?;
    m.add_class::<Side>()?;
    m.add_class::<Submit>()?;
    Ok(())
}