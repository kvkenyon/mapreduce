//! src/worker/executor.rs
use crate::emitter::PartitionedFileEmitter;
use crate::mappers::{Mapper, WordCounter};
use crate::master::MapTask;
use crate::storage::S3Storage;
use anyhow::Context;

#[derive(Debug)]
pub struct MapExecutor {
    task: MapTask,
    emitter: PartitionedFileEmitter,
}

impl MapExecutor {
    pub async fn new(task: &MapTask) -> anyhow::Result<Self> {
        let emitter = PartitionedFileEmitter::new(task.task_id, task.r, 2048)
            .await
            .context("failed to build emitter")?;

        Ok(Self {
            task: task.clone(),
            emitter,
        })
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
