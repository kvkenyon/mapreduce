//! src/api/tests/mapreduce.rs
use claims::{assert_none, assert_some};
use mapreduce::spec::{MapReduceInput, MapReduceOutput, MapReduceSpecification};

#[test]
fn you_should_be_able_to_create_a_map_reduce_sepc() {
    let spec = MapReduceSpecification::new(2, 16, 16);
    assert_eq!(spec.machines(), 2);
    assert_eq!(spec.map_megabytes(), 16);
    assert_eq!(spec.reduce_megabytes(), 16);
}

#[test]
fn you_should_be_able_to_add_input_to_a_map_reduce_spec() {
    let mut spec = MapReduceSpecification::new(2, 16, 16);

    for i in 0..5 {
        let input = MapReduceInput::new(
            "text".to_string(),
            format!("input_{i}.txt"),
            "WordCounter".to_string(),
        );
        spec.add_input(input);
    }

    assert_eq!(spec.inputs().len(), 5);

    for (i, input) in spec.inputs().iter().enumerate() {
        assert_eq!(input.filename(), format!("input_{i}.txt"));
        assert_eq!(input.format(), "text".to_string());
        assert_eq!(input.mapper(), "WordCounter");
    }
}

#[test]
fn you_should_be_able_to_define_a_map_reduce_output_on_a_spec() {
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
        "/root/home/word_counts".to_string(),
        100,
        "text".to_string(),
        "Adder".to_string(),
        None,
    );

    spec.set_output(output);

    assert_some!(spec.output());

    match spec.output() {
        None => panic!("Error: Should be some."),
        Some(output) => {
            assert_eq!(output.base_path(), "/root/home/word_counts");
            assert_eq!(output.format(), "text");
            assert_eq!(output.num_tasks(), 100);
            assert_none!(output.combiner());
            assert_eq!(output.reducer(), "Adder");
        }
    }
}
