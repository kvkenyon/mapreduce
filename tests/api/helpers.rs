//! src/tests/api/helpers.rs
use claims::assert_none;
use claims::assert_some;
use mapreduce::spec::{
    MapReduceInput, MapReduceInputFormat, MapReduceOutput, MapReduceSpecification,
};

use std::path::PathBuf;

pub fn test_data_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path
}

pub fn build_spec() -> MapReduceSpecification {
    let mut spec = MapReduceSpecification::new(2, 16, 16);

    for i in 0..5 {
        let mut path = test_data_dir();
        path.push(format!("input_{i}.txt"));
        let input = MapReduceInput::new(
            MapReduceInputFormat::Text,
            path.to_str().unwrap().to_string(),
            "WordCounter".to_string(),
        );
        spec.add_input(input);
    }

    assert_none!(spec.output());

    let output = MapReduceOutput::new(
        "/word_counts".to_string(),
        100,
        mapreduce::spec::MapReduceOutputFormat::Text,
        "Adder".to_string(),
        None,
    );

    spec.set_output(output);

    assert_some!(spec.output());
    spec
}
