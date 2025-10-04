//! src/reducers/mod.rs
pub trait ReduceEmitter {}

pub trait Reducer {
    fn reduce(
        &mut self,
        key: &str,
        values: Box<dyn Iterator<Item = &str> + '_ + Send + Sync>,
    ) -> impl Future<Output = ()>;
    fn emit(&mut self, value: &str) -> impl Future<Output = ()>;
}

mod adder;

pub use adder::Adder;
