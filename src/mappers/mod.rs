//! src/mappers/mod.rs

pub trait Mapper {
    fn map(&mut self, key: &str, value: &str);
}

mod word_counter;
