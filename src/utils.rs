//! src/util.rs
use crate::functions::{
    DefaultReduceEmitter, FileMapEmitter, Key, MapEmitter, Mapper, ReduceEmitter, Reducer, Value,
};

pub struct WordCounter {
    emitter: FileMapEmitter,
}

pub struct Adder {
    emitter: DefaultReduceEmitter,
}

impl Mapper for WordCounter {
    type Emitter = FileMapEmitter;
    fn build(emitter: FileMapEmitter) -> Self {
        Self { emitter }
    }

    fn map(&mut self, _key: Key, value: Value) {
        for word in value.to_lowercase().split(|c: char| !c.is_alphabetic()) {
            if !word.is_empty() {
                self.emitter.emit(word.to_string(), "1".to_string());
            }
        }
    }
}

impl Reducer for Adder {
    type Emitter = DefaultReduceEmitter;
    fn build(emitter: DefaultReduceEmitter) -> Self {
        Self { emitter }
    }

    fn reduce<I: Iterator<Item = Value>>(&self, _key: Key, values: I) {
        self.emitter.emit(values.count().to_string());
    }
}
