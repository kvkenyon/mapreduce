//! src/test_utils.rs
use crate::mapreduce::MapReduce;
use crate::spec::*;
use crate::spec::{MapReduceInput, MapReduceOutput, MapReduceSpecification};
use crate::utils::{Adder, WordCounter};
use crate::{impl_mapper, impl_reducer};
#[cfg(test)]
use std::path::PathBuf;

pub fn test_data_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path
}

#[allow(unused)]
pub fn make_mr_job() -> MapReduce {
    let mut spec = MapReduceSpecification::new(3, 1, 1);
    let mut path = test_data_dir();
    path.push("small_test.txt");
    spec.add_input(MapReduceInput::new(
        MapReduceInputFormat::Text,
        path.to_str().unwrap().to_string(),
        "word_counter".to_string(),
    ));
    spec.set_output(MapReduceOutput::new(
        "/tmp/mapreduce/out".to_string(),
        2,
        MapReduceOutputFormat::Text,
        "adder".to_string(),
        None,
    ));
    let mr = MapReduce::new(spec).expect("Failed to create map reduce job");
    impl_mapper!(WordCounter, "word_counter", 2);
    impl_reducer!(Adder, "adder");
    mr
}
