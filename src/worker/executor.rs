//! src/worker/executor.rs
use crate::mappers::{Mapper, WordCounter};
use crate::master::{MapTask, ReduceTask};
use crate::reducers::{Adder, Reducer};
use crate::storage::S3Storage;
use anyhow::Context;

#[derive(Debug)]
pub struct MapExecutor {
    task: MapTask,
}

impl MapExecutor {
    pub async fn new(task: &MapTask) -> anyhow::Result<Self> {
        Ok(Self { task: task.clone() })
    }

    #[tracing::instrument("Execute map", skip_all, fields(
        task_id = %self.task.task_id,
        mapper = %self.task.input_split.mapper(),
        input_split_key = %self.task.input_split.key(),
        bucket_name = %self.task.input_split.bucket_name()
    ))]
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mapper = self.task.input_split.mapper();
        let s3 = S3Storage::new(self.task.input_split.bucket_name()).await?;
        let input = s3.get(self.task.input_split.key()).await?;
        tracing::info!("input = {input}");
        // TODO: Add registry for mappers to avoid hard coding if-else branches
        if mapper == "WordCounter" {
            tracing::info!("executing mapper - WordCounter");
            let mut mapper = WordCounter::new(self.task.task_id, self.task.r, 2048)
                .await
                .context("Failed to build mapper")?;
            mapper.map("", &input).await;
            tracing::info!("mapper done");
            mapper.finish().await?;
            tracing::info!("mapper finished");
        } else {
            tracing::warn!("Invalid mapper, bailing...");
            anyhow::bail!("Invalid mapper provided: {}", mapper);
        }
        tracing::info!("execute done");
        Ok(())
    }
}

#[derive(Debug)]
pub struct ReduceExecutor {
    task: ReduceTask,
    out: Vec<u8>,
}

impl ReduceExecutor {
    pub fn new(task: ReduceTask) -> anyhow::Result<Self> {
        Ok(Self {
            task,
            out: Vec::new(),
        })
    }

    #[tracing::instrument("Execute reduce task", skip_all, fields(
        task_id = %self.task.task_id,
        input_loc = %self.task.input_location.clone().unwrap_or("None".to_string()),
        output_loc = %self.task.output.base_path(),
        r = %self.task.r,
        reducer = %self.task.output.reducer()
    ))]
    pub async fn execute(&self) -> anyhow::Result<()> {
        let input_prefix = format!("mr_input_{}", self.task.r);
        let input_location = if let Some(input_location) = self.task.input_location.clone() {
            input_location
        } else {
            anyhow::bail!("No input location specified");
        };
        let s3 = S3Storage::new(input_location.as_ref()).await?;

        let keys = s3.list().await?;

        tracing::info!("keys={keys:?}");

        let keys = keys
            .iter()
            .filter(|key| key.starts_with::<&str>(input_prefix.as_ref()));

        tracing::info!("filtered keys={keys:?}");

        let mut input = String::new();

        for key in keys {
            let data = s3.get(key).await?;
            input.push_str(data.as_ref());
        }

        let mut key_value_pairs: Vec<&str> = input.split("\n").filter(|p| !p.is_empty()).collect();

        key_value_pairs.sort();
        tracing::info!("{key_value_pairs:?}");

        if self.task.output.reducer() == "Adder" {
            let mut adder = Adder::new(self.task.output.base_path().into(), self.task.r);
            let mut intermediate_pairs: Vec<&str> = vec![];
            let mut prev: &str = key_value_pairs.first().unwrap();
            intermediate_pairs.push(prev);
            for pair in key_value_pairs[1..].into_iter() {
                if prev == *pair {
                    intermediate_pairs.push(pair);
                } else {
                    let p = intermediate_pairs.first().unwrap();
                    let idx = p.find(",").unwrap();
                    let key = &p[1..idx];
                    tracing::info!("Reducing: {intermediate_pairs:?} with key = {key}");
                    adder
                        .reduce(key, Box::new(intermediate_pairs.into_iter()))
                        .await;
                    intermediate_pairs = vec![pair];
                }
                prev = pair;
            }
            adder.finish().await?;
        }
        Ok(())
    }
}
