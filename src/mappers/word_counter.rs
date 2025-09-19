//! src/mappers/word_counter.rs
use crate::emitter::map_emit;
use crate::mappers::Mapper;

#[derive(Debug)]
#[allow(unused)]
pub struct WordCounter;

impl Mapper for WordCounter {
    #[tracing::instrument(name = "WordCounter map", skip(value))]
    fn map(&mut self, _key: &str, value: &str) {
        for word in value.to_lowercase().split(|c: char| !c.is_alphabetic()) {
            if !word.is_empty() {
                map_emit(word, "1");
            }
        }
    }
}
