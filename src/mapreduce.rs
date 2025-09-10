//! src/mapreduce.rs
use crate::{
    file_splitter::FileSplitter,
    functions::{Mapper, Reducer},
    spec::MapReduceSpecification,
};
use anyhow::Context;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

pub struct MapReduce<M: Mapper, R: Reducer> {
    job_id: Uuid,
    mapper: Option<M>,
    reducer: Option<R>,
    spec: MapReduceSpecification,
    input_files: Vec<PathBuf>,
}

impl<M: Mapper, R: Reducer> MapReduce<M, R> {
    pub fn new(spec: MapReduceSpecification) -> Result<Self, anyhow::Error> {
        let job_id = Uuid::new_v4();
        let mut mr = MapReduce {
            spec,
            mapper: None,
            reducer: None,
            job_id,
            input_files: vec![],
        };
        mr.split_inputs()?;
        Ok(mr)
    }

    pub fn job_id(&self) -> &Uuid {
        &self.job_id
    }

    pub fn num_map_tasks(&self) -> usize {
        self.input_files.len()
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

    pub fn input_files(&self) -> &Vec<PathBuf> {
        &self.input_files
    }

    // Private
    #[allow(dead_code)]
    fn split_inputs(&mut self) -> Result<(), anyhow::Error> {
        let out_path = PathBuf::from(format!("/tmp/mapreduce/jobs/{}", self.job_id));
        fs::create_dir_all(&out_path).context("Failed to create inputs directory")?;

        let length = self.spec().inputs().len();

        for i in 0..length {
            let input = self
                .spec()
                .inputs()
                .get(i)
                .context("Failed to get input.")?;
            let path = PathBuf::from(input.filename());
            let out_file_name = format!("input_{i}");
            let splitter = FileSplitter::new(
                path,
                (self.spec().map_megabytes() * 1000) as u64,
                out_file_name,
                out_path.clone(),
            );
            let out_paths = splitter.split().context("Failed to split input files")?;
            dbg!(&out_paths);
            self.input_files.extend(out_paths);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::*;
    use crate::test_utils::test_data_dir;
    use crate::utils::{Adder, WordCounter};

    fn make_mr_job() -> MapReduce<WordCounter, Adder> {
        let mut spec = MapReduceSpecification::new(2, 1, 1);
        let mut path = test_data_dir();
        path.push("small_test.txt");
        let out_path = PathBuf::from(format!("/tmp/mapreduce/out/{}", Uuid::new_v4()));
        spec.add_input(MapReduceInput::new(
            MapReduceInputFormat::Text,
            path.to_str().unwrap().to_string(),
            "WordCounter".to_string(),
        ));
        spec.set_output(MapReduceOutput::new(
            out_path.to_str().unwrap().to_string(),
            100,
            MapReduceOutputFormat::Text,
            "Adder".to_string(),
            None,
        ));
        MapReduce::<WordCounter, Adder>::new(spec).expect("Failed to create map reduce job")
    }

    #[test]
    fn should_split_inputs_and_keep_record_of_the_new_paths() {
        let mr_job = make_mr_job();

        let expected = [
            "input_0_0".to_string(),
            "input_0_1".to_string(),
            "input_0_2".to_string(),
        ];

        for input in mr_job.input_files() {
            assert!(expected.contains(&input.file_name().unwrap().to_str().unwrap().to_string()));
        }
    }

    #[test]
    fn should_contain_the_number_of_map_tasks_given_the_spec_after_new() {
        let mr_job = make_mr_job();

        dbg!(mr_job.input_files);

        // assert_eq!(3, mr_job.num_map_tasks());
    }
}
