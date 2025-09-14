//! src/mappers/mod.rs
pub trait MapEmitter {
    fn emit(&mut self, key: &str, value: &str);
}

pub trait Mapper {
    fn map(&mut self, key: &str, value: &str);
}

mod word_counter;
