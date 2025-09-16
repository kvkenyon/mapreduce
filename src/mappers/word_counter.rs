//! src/mappers/word_counter.rs
use crate::mappers::{MapEmitter, Mapper};

#[derive(Debug)]
#[allow(unused)]
pub struct WordCounter;

impl MapEmitter for WordCounter {
    #[tracing::instrument(name = "WordCounter emit")]
    fn emit(&mut self, key: &str, value: &str) {
        tracing::info!("({}, {})", key, value);
    }
}

impl Mapper for WordCounter {
    #[tracing::instrument(name = "WordCounter map", skip(value))]
    fn map(&mut self, _key: &str, value: &str) {
        for word in value.to_lowercase().split(|c: char| !c.is_alphabetic()) {
            if !word.is_empty() {
                self.emit(word, "1");
            }
        }
    }
}
