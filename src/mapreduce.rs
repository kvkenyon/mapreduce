//! src/mapreduce.rs
use crate::{
    functions::{Mapper, Reducer},
    spec::MapReduceSpecification,
};
use anyhow::Context;
use std::fs;

pub struct MapReduce<M: Mapper, R: Reducer> {
    mapper: Option<M>,
    reducer: Option<R>,
    spec: MapReduceSpecification,
}

impl<M: Mapper, R: Reducer> MapReduce<M, R> {
    pub fn new(spec: MapReduceSpecification) -> Self {
        MapReduce {
            spec,
            mapper: None,
            reducer: None,
        }
    }

    pub fn register_mapper(&mut self, m: M)
    where
        M: Mapper,
    {
        self.mapper = Some(m);
    }

    pub fn mapper(&self) -> Option<&M> {
        match &self.mapper {
            Some(m) => Some(m),
            None => None,
        }
    }

    pub fn register_reducer(&mut self, r: R)
    where
        R: Reducer,
    {
        self.reducer = Some(r);
    }

    pub fn reducer(&self) -> Option<&R> {
        match &self.reducer {
            Some(r) => Some(r),
            None => None,
        }
    }

    pub fn spec(&self) -> &MapReduceSpecification {
        &self.spec
    }

    // Private
    #[allow(dead_code)]
    fn split_inputs(&self) -> Result<(), anyhow::Error> {
        if !self.spec.inputs().is_empty() {
            for input in self.spec.inputs() {
                let filename = input.filename();
                let result = fs::read_to_string(filename)
                    .context(format!("Failed to read file {filename}"))?;
                println!("read file {filename} with size {}", result.len());
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to split inputs"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_data_dir;
    use crate::{
        spec::MapReduceInput,
        util::{Adder, WordCounter},
    };
    use claims::{assert_err, assert_ok};

    #[test]
    fn split_inputs_fails_when_no_inputs_added() {
        let spec = MapReduceSpecification::new(2, 16, 16);
        let job = MapReduce::<WordCounter, Adder>::new(spec);
        assert_err!(job.split_inputs());
    }

    #[test]
    fn split_inputs_should_fail_gracefully_when_input_file_cant_be_read() {
        let mut spec = MapReduceSpecification::new(2, 16, 16);
        spec.add_input(MapReduceInput::new(
            "text".to_string(),
            "non-existant-input-file.txt".to_string(),
            "WordCounter".to_string(),
        ));
        let job = MapReduce::<WordCounter, Adder>::new(spec);
        assert_err!(job.split_inputs());
    }

    #[test]
    fn should_split_input_into_m_pieces_based_on_user_provided_spec() {
        let mut filename = test_data_dir();
        filename.push("small_test.txt");
        let mut spec = MapReduceSpecification::new(2, 16, 16);
        spec.add_input(MapReduceInput::new(
            "text".to_string(),
            filename.to_str().unwrap().to_string(),
            "WordCounter".to_string(),
        ));
        let job = MapReduce::<WordCounter, Adder>::new(spec);

        assert_ok!(job.split_inputs());
    }
}
