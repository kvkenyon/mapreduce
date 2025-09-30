//! src/mappers/word_counter.rs
use crate::emitter::PartitionedFileEmitter;
use crate::mappers::Mapper;
use anyhow::Context;
use uuid::Uuid;

#[derive(Debug)]
pub struct WordCounter {
    emitter: PartitionedFileEmitter,
}

impl WordCounter {
    #[tracing::instrument("WordCounter new")]
    pub async fn new(task_id: Uuid, r: usize, buf_size: usize) -> anyhow::Result<Self> {
        let emitter = PartitionedFileEmitter::new(task_id, r, buf_size)
            .await
            .context("failed to create partitioned file emitter")?;
        Ok(Self { emitter })
    }

    pub async fn finish(&mut self) -> anyhow::Result<()> {
        self.emitter
            .flush_all()
            .await
            .context("Failed to flush all")?;
        Ok(())
    }
}

impl Mapper for WordCounter {
    #[tracing::instrument(name = "WordCounter map", skip(self, value))]
    async fn map(&mut self, _key: &str, value: &str) {
        tracing::info!("map for value = {value}");
        for word in value.to_lowercase().split(|c: char| !c.is_alphabetic()) {
            if !word.is_empty() {
                tracing::info!("Emit: ({word}, \"1\")");
                self.emit(word, "1").await;
            }
        }
    }

    #[tracing::instrument("Emitting", skip(self))]
    async fn emit(&mut self, key: &str, value: &str) {
        tracing::info!("Calling emitter.");
        self.emitter.emit(key, value).await.expect("failed to emit")
    }
}
