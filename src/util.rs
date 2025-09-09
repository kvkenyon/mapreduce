//! src/util.rs
use crate::functions::{
    DefaultMapEmitter, DefaultReduceEmitter, Key, MapEmitter, Mapper, ReduceEmitter, Reducer, Value,
};

pub struct WordCounter {
    emitter: DefaultMapEmitter,
}

pub struct Adder {
    emitter: DefaultReduceEmitter,
}

impl Mapper for WordCounter {
    type Emitter = DefaultMapEmitter;
    fn build(emitter: DefaultMapEmitter) -> Self {
        Self { emitter }
    }

    fn map(&self, _key: Key, value: Value) {
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
