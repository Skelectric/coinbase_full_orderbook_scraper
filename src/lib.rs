pub mod avl_tree_py;
pub mod orderbook_py;

use pyo3::prelude::*;
use orderbook_py::*;

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