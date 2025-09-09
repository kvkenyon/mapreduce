//! src/functions.rs

pub type Key = String;
pub type Value = String;

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
    fn emit(&self, key: Key, value: Value);
}

pub trait Mapper {
    type Emitter: MapEmitter;
    fn build(emitter: Self::Emitter) -> Self;
    fn map(&self, key: Key, value: Value);
}

pub trait ReduceEmitter {
    fn emit(&self, value: Value);
}

pub trait Reducer {
    type Emitter: ReduceEmitter;
    fn build(emitter: Self::Emitter) -> Self;
    fn reduce<I: Iterator<Item = Value>>(&self, key: Key, values: I);
}

pub struct DefaultMapEmitter;
pub struct DefaultReduceEmitter;

impl MapEmitter for DefaultMapEmitter {
    fn emit(&self, key: Key, value: Value) {
        println!("({key}, {value})");
    }
}

impl ReduceEmitter for DefaultReduceEmitter {
    fn emit(&self, value: Value) {
        println!("({value})");
    }
}
