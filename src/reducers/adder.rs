//! src/reducers/adder.rs
use crate::reducers::{ReduceEmitter, Reducer};

#[allow(unused)]
pub struct Adder;

impl ReduceEmitter for Adder {
    fn emit(&self, value: &str) {
        println!("{value}");
    }
}

impl Reducer for Adder {
    fn reduce(&self, key: &str, values: Box<dyn Iterator<Item = String>>) {
        self.emit(&format!("({}, {})", key, &values.count()));
    }
}
