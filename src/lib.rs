extern crate alloc;

// pub mod avl_tree;
// pub mod orderbook;
pub mod orderbook_py;
mod avl_tree_py;

use pyo3::prelude::*;

/// An unsafe AVL-tree limit orderbook written in Rust
///
/// No UB and thread-safety not guaranteed
#[pymodule]
fn rust_orderbook(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    // m.add_class::<LimitOrderbook>()?;
    Ok(())
}


/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}