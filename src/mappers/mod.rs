//! src/mappers/mod.rs
pub use word_counter::WordCounter;

pub trait Mapper {
    fn map(&mut self, key: &str, value: &str) -> impl Future<Output = ()>;
    fn emit(&mut self, key: &str, value: &str) -> impl Future<Output = ()>;
}

mod word_counter;
