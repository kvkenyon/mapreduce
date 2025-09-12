//! src/mapreduce.rs
use crate::{
    file_splitter::FileSplitter,
    master::Master,
    spec::{MapReduceInput, MapReduceSpecification},
    worker::Worker,
};
use anyhow::Context;
use std::path::PathBuf;
use std::{collections::HashMap, fs};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct InputSplit {
    id: Uuid,
    location: PathBuf,
    function: String,
}

impl InputSplit {
    pub fn new(path: PathBuf, function: String) -> Self {
        InputSplit {
            id: Uuid::new_v4(),
            location: path,
            function,
        }
    }

    pub fn location(&self) -> &PathBuf {
        &self.location
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn function(&self) -> &str {
        &self.function
    }
}

fn split_inputs(
    job_id: Uuid,
    inputs: &[MapReduceInput],
    split_size_mb: u64,
) -> Result<HashMap<Uuid, InputSplit>, anyhow::Error> {
    let out_path = PathBuf::from(format!("/tmp/mapreduce/jobs/{}", job_id));
    fs::create_dir_all(&out_path).context("Failed to create inputs directory")?;

    let length = inputs.len();

    let mut results = HashMap::<Uuid, InputSplit>::new();
    for i in 0..length {
        let input = inputs.get(i).context("Failed to get input.")?;
        let path = PathBuf::from(input.filename());
        let out_file_name = format!("input_{i}");
        let splitter = FileSplitter::new(
            path.clone(),
            split_size_mb * 1000,
            out_file_name,
            out_path.clone(),
        );
        let out_paths = splitter.split().context("Failed to split input files")?;
        for out_path in out_paths {
            let input_split = InputSplit::new(out_path, input.mapper().to_string());
            results.insert(*input_split.id(), input_split);
        }
    }
    Ok(results)
}

pub struct MapReduce {
    job_id: Uuid,
    spec: MapReduceSpecification,
    master: Master,
}

impl MapReduce {
    pub fn new(spec: MapReduceSpecification) -> Result<Self, anyhow::Error> {
        let job_id = Uuid::new_v4();
        let split_size_mb = spec.map_megabytes();
        let inputs = spec.inputs();

        let input_splits = split_inputs(job_id, inputs, split_size_mb.into())?;

        let output = spec.output().ok_or(anyhow::anyhow!(
            "No reduce output configured in map reduce specification"
        ))?;

        let mut workers = vec![];

        let num_workers = spec.machines();

        for _ in 0..num_workers {
            workers.push(Worker::new());
        }

        let master = Master::new(workers, input_splits, output);

        let mr = MapReduce {
            spec,
            job_id,
            master,
        };

        Ok(mr)
    }

    pub fn master(&self) -> &Master {
        &self.master
    }

    pub fn master_mut(&mut self) -> &mut Master {
        &mut self.master
    }

    pub fn job_id(&self) -> &Uuid {
        &self.job_id
    }

    pub fn spec(&self) -> &MapReduceSpecification {
        &self.spec
    }

    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        self.master.run()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::make_mr_job;

    #[test]
    fn should_split_inputs_and_keep_record_of_the_new_paths() {
        let mr_job = make_mr_job();

        let expected = [
            "input_0_0".to_string(),
            "input_0_1".to_string(),
            "input_0_2".to_string(),
        ];
        for split in mr_job.master().input_splits() {
            assert!(
                expected.contains(
                    &split
                        .location
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string()
                )
            )
        }
    }

    #[test]
    fn should_contain_the_number_of_map_and_reduce_tasks_given_the_spec_after_new() {
        let mr_job = make_mr_job();
        assert_eq!(5, mr_job.master.task_count());
    }

    #[test]
    fn should_create_master_and_workers() {
        let mr_job = make_mr_job();
        assert_eq!(mr_job.master().worker_count(), 3);
    }

    #[test]
    fn run_kicks_off_execution() {
        let mut mr_job = make_mr_job();
        mr_job.run().expect("Failed to run jobs");
    }
}
