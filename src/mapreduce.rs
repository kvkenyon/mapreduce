//! src/mapreduce.rs
use crate::{
    file_splitter::AsyncFileSplitter,
    spec::{MapReduceInput, MapReduceSpecification},
};
use anyhow::Context;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
pub struct MapInput {
    key: String,
    value: String,
}

impl MapInput {
    pub fn new(key: &str, value: &str) -> Self {
        MapInput {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

pub struct ReduceInput {
    key: String,
    values: Box<dyn Iterator<Item = String>>,
}

impl ReduceInput {
    pub fn new(key: &str, values: Box<dyn Iterator<Item = String>>) -> Self {
        ReduceInput {
            key: key.to_string(),
            values,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn into_values(self) -> Box<dyn Iterator<Item = String>> {
        self.values
    }
}

#[derive(Clone, Debug)]
pub struct InputSplit {
    id: Uuid,
    key: String,
    mapper: String,
}

impl InputSplit {
    pub fn new(key: &str, mapper: &str) -> Self {
        InputSplit {
            id: Uuid::new_v4(),
            key: key.into(),
            mapper: mapper.into(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn mapper(&self) -> &str {
        &self.mapper
    }
}

#[tracing::instrument(name = "Split inputs")]
async fn split_inputs(
    job_id: &str,
    bucket_name: &str,
    inputs: &[MapReduceInput],
    out_key: &str,
    split_size_in_bytes: u32,
) -> Result<HashMap<String, Vec<InputSplit>>, anyhow::Error> {
    let length = inputs.len();
    let mut results = HashMap::<String, Vec<InputSplit>>::new();
    for i in 0..length {
        let input = inputs.get(i).context("Failed to get input.")?;
        tracing::debug!("processing input file: {}", input.filename());
        let mut splitter = AsyncFileSplitter::new(
            job_id,
            bucket_name,
            input.filename(),
            &format!("{}_{}_of_{}", out_key, i, length - 1),
            split_size_in_bytes,
        )?;
        let result_keys = splitter
            .split()
            .await
            .context("Failed to split input files")?;
        let mut input_splits = vec![];
        for result_key in result_keys {
            let input_split = InputSplit::new(&result_key, input.mapper());
            input_splits.push(input_split);
        }
        results.insert(input.filename().to_string(), input_splits);
    }
    Ok(results)
}

#[allow(unused)]
pub struct MapReduce {
    job_id: Uuid,
    spec: MapReduceSpecification,
    input_splits: HashMap<String, Vec<InputSplit>>,
}

impl MapReduce {
    pub async fn new(spec: MapReduceSpecification) -> Result<Self, anyhow::Error> {
        let job_id = Uuid::new_v4();
        let inputs = spec.inputs();
        let input_splits = split_inputs(
            &job_id.to_string(),
            spec.bucket_name(),
            inputs,
            "mr_input",
            spec.map_megabytes(),
        )
        .await?;
        Ok(MapReduce {
            spec,
            job_id,
            input_splits,
        })
    }

    pub fn spec(&self) -> &MapReduceSpecification {
        &self.spec
    }

    pub fn input_splits(&self) -> &HashMap<String, Vec<InputSplit>> {
        &self.input_splits
    }

    pub fn job_id(&self) -> &Uuid {
        &self.job_id
    }
}
