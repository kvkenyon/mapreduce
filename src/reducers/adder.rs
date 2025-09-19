//! src/reducers/adder.rs
use crate::emitter::reduce_emit;
use crate::reducers::Reducer;

#[derive(Debug)]
#[allow(unused)]
pub struct Adder;

impl Reducer for Adder {
    #[tracing::instrument("Adder", skip_all)]
    fn reduce(&self, _: &str, values: Box<dyn Iterator<Item = String>>) {
        reduce_emit(&format!("{}", &values.count()));
    }
}
