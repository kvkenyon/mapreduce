//! src/mapreduce.rs
use crate::{
    file_splitter::FileSplitter,
    spec::{MapReduceInput, MapReduceSpecification},
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

#[allow(unused)]
pub struct MapReduce {
    job_id: Uuid,
    spec: MapReduceSpecification,
    input_splits: HashMap<Uuid, InputSplit>,
}

impl MapReduce {
    pub fn new(spec: MapReduceSpecification) -> Result<Self, anyhow::Error> {
        let job_id = Uuid::new_v4();
        let split_size_mb = spec.map_megabytes();
        let inputs = spec.inputs();

        let input_splits = split_inputs(job_id, inputs, split_size_mb.into())?;

        Ok(MapReduce {
            spec,
            job_id,
            input_splits,
        })
    }
}
