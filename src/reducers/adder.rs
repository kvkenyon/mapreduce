//! src/reducers/adder.rs
use crate::reducers::{ReduceEmitter, Reducer};

#[derive(Debug)]
#[allow(unused)]
pub struct Adder;

impl ReduceEmitter for Adder {
    #[tracing::instrument]
    fn emit(&self, value: &str) {
        tracing::info!("{value}");
    }
}

impl Reducer for Adder {
    #[tracing::instrument(skip(values))]
    fn reduce(&self, key: &str, values: Box<dyn Iterator<Item = String>>) {
        self.emit(&format!("({}, {})", key, &values.count()));
    }
}
