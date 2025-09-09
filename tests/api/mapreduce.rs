//! src/tests/api/mapreduce.rs
use crate::helpers::build_spec;
use claims::assert_some;
use mapreduce::{
    functions::{
        DefaultMapEmitter, DefaultReduceEmitter, Key, MapEmitter, Mapper, ReduceEmitter, Reducer,
        Value,
    },
    mapreduce::MapReduce,
};

struct WordCounter {
    emitter: DefaultMapEmitter,
}
struct Adder {
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

#[test]
fn should_be_able_to_register_mapper_and_reducer_on_map_reduce_job() {
    let spec = build_spec();
    let mut job = MapReduce::<WordCounter, Adder>::new(spec);

    let word_counter = WordCounter::build(DefaultMapEmitter);
    let adder = Adder::build(DefaultReduceEmitter);

    job.register_mapper(word_counter);
    job.register_reducer(adder);

    assert_some!(job.mapper());
    assert_some!(job.reducer());
}
