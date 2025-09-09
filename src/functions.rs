//! src/functions.rs

type Key = String;
type Value = String;

pub struct MapInput {
    key: Key,
    value: Value,
}

impl MapInput {
    pub fn new(key: Key, value: Value) -> Self {
        MapInput { key, value }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

pub struct ReduceInput<I: Iterator<Item = Value>> {
    key: Key,
    values: I,
}

impl<I: Iterator<Item = Value>> ReduceInput<I> {
    pub fn new(key: Key, values: I) -> Self {
        ReduceInput { key, values }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn into_values(self) -> I {
        self.values
    }
}

pub trait MapEmitter {
    fn emit(key: Key, value: Value);
}

pub trait Mapper {
    fn build(emitter: impl MapEmitter) -> Self;
    fn map(&self, key: Key, value: Value);
}

pub trait ReduceEmitter {
    fn emit(value: Value);
}

pub trait Reducer {
    fn build(emitter: impl ReduceEmitter) -> Self;
    fn reduce<I: Iterator<Item = Value>>(&self, key: Key, values: I);
}
