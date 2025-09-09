//! src/tests/api/helpers.rs
use claims::assert_none;
use claims::assert_some;
use mapreduce::spec::{MapReduceInput, MapReduceOutput, MapReduceSpecification};

pub fn build_spec() -> MapReduceSpecification {
    let mut spec = MapReduceSpecification::new(2, 16, 16);

    for i in 0..5 {
        let input = MapReduceInput::new(
            "text".to_string(),
            format!("input_{i}.txt"),
            "WordCounter".to_string(),
        );
        spec.add_input(input);
    }

    assert_none!(spec.output());

    let output = MapReduceOutput::new(
        "/word_counts".to_string(),
        100,
        "text".to_string(),
        "Adder".to_string(),
        None,
    );

    spec.set_output(output);

    assert_some!(spec.output());
    spec
}
