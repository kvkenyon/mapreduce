//! src/reducers/mod.rs
pub trait ReduceEmitter {
    fn emit(&self, value: &str);
}

pub trait Reducer {
    fn reduce(&self, key: &str, values: Box<dyn Iterator<Item = String>>);
}

mod adder;
