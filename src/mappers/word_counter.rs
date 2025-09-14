//! src/mappers/word_counter.rs
use crate::mappers::{MapEmitter, Mapper};

#[allow(unused)]
pub struct WordCounter;

impl MapEmitter for WordCounter {
    fn emit(&mut self, key: &str, value: &str) {
        println!("({}, {})", key, value);
    }
}

impl Mapper for WordCounter {
    fn map(&mut self, _key: &str, value: &str) {
        for word in value.to_lowercase().split(|c: char| !c.is_alphabetic()) {
            if !word.is_empty() {
                self.emit(word, "1");
            }
        }
    }
}
