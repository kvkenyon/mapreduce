//! src/emitter.rs

use crate::storage::S3Storage;
use anyhow::Context;
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use uuid::Uuid;

pub fn map_emit(key: &str, value: &str) {
    tracing::info!("({key}, {value})");
}

pub fn reduce_emit(value: &str) {
    tracing::info!("{value}");
}

/// Given a number of reduce tasks R the partitioned file based emitter should use the input key
/// and pass it to a part_func(k) % R returning r. Then emit all outputs to a buffered writer for each r.
/// When the data of the next write exceeds the buffer size, we flush the buffer to a local tmp file.
/// at /tmp/mapreduce/<task-id>/<mr_input_{r}>
/// then put that file into cloud storage with a part number which is incremented after each
/// successful store. Each r has a part.
/// s3://<bucket_name>/mr_input_{r}_{part}
/// We must call flush_all on the emitter since buffers may still be filled and not flushed.
pub struct PartitionedFileEmitter {
    task_id: Uuid,
    r: usize,
    parts: Vec<usize>,
    buf_writers: Vec<BufWriter<File>>,
    buf_size: usize,
    storage_handle: S3Storage,
    storage_keys: Vec<String>,
}

impl PartitionedFileEmitter {
    const MR_INPUT: &'static str = "mr_input_";
    const LOCAL_FILE_PATH: &'static str = "/tmp/mapreduce/";

    #[tracing::instrument("Create new partitioned file emitter")]
    pub async fn new(task_id: Uuid, r: usize, buf_size: usize) -> anyhow::Result<Self> {
        let mut buf_writers = Vec::with_capacity(r);
        let mut parts = Vec::with_capacity(r);

        let span = tracing::debug_span!("Create local tmp storage");
        span.in_scope(|| -> anyhow::Result<()> {
            let path = format!("{}{task_id}/", Self::LOCAL_FILE_PATH);
            tracing::debug!("create directory at: {path:?}");
            std::fs::create_dir_all(&path)
                .context("Failed to create intermediate file directory")?;
            let mut path = PathBuf::from(path);
            for i in 0..r {
                path.push(format!("{}{i}", Self::MR_INPUT));
                let file =
                    File::create(&path).context("Failed to create intermediate output file")?;
                let buf_writer = BufWriter::with_capacity(2048, file);
                buf_writers.push(buf_writer);
                path.pop();
                parts.push(0);
            }
            Ok(())
        })?;

        let storage_handle = S3Storage::new(&task_id.to_string())
            .await
            .context("Failed to acquire s3 storage handle")?;

        Ok(Self {
            task_id,
            r,
            parts,
            buf_writers,
            storage_keys: vec![],
            buf_size,
            storage_handle,
        })
    }

    fn part_func(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() % self.r as u64
    }

    fn get_buf_writer(&mut self, r: usize) -> &mut BufWriter<File> {
        self.buf_writers.get_mut(r).unwrap()
    }

    fn inc_part(&mut self, r: usize) {
        let curr_part = self.parts.get(r).unwrap();
        self.parts[r] = *curr_part + 1;
    }

    pub fn storage_keys(&self) -> &Vec<String> {
        &self.storage_keys
    }

    #[tracing::instrument("Get local file path", skip(self))]
    fn get_local_file_path(&self, r: usize) -> PathBuf {
        PathBuf::from(format!(
            "{}{}/{}{r}",
            Self::LOCAL_FILE_PATH,
            self.task_id,
            Self::MR_INPUT
        ))
    }

    fn get_buffer_len(&self, r: usize) -> usize {
        self.buf_writers.get(r).unwrap().buffer().len()
    }

    async fn flush_or_write(&mut self, r: usize, data: &[u8]) -> Result<(), anyhow::Error> {
        if self.get_buffer_len(r) + data.len() > self.buf_size {
            self.flush(r).await.context(format!("Flushing {r}"))?;
        }
        self.get_buf_writer(r)
            .write_all(data)
            .context("Failed to write to buf writer")?;
        Ok(())
    }

    #[tracing::instrument("Flush", skip(self))]
    pub async fn flush(&mut self, r: usize) -> anyhow::Result<()> {
        let file_path = self.get_local_file_path(r);
        let buf_writer = self.get_buf_writer(r);
        buf_writer.flush().context("Failed to flush writer.")?;
        let mut file = File::options()
            .read(true)
            .write(true)
            .open(file_path)
            .context("Failed to open file.")?;
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)
            .context("Failed to read file into buf")?;
        let part = self.parts.get(r).unwrap();
        let key = format!("mr_input_{r}_{part}");
        self.storage_handle
            .put(&key, &buf)
            .await
            .context("Failed to write to s3")?;
        file.set_len(0).context("Failed to clear local file")?;
        file.seek(SeekFrom::Start(0))?;
        self.buf_writers[r] = BufWriter::with_capacity(self.buf_size, file);
        self.storage_keys.push(key);
        self.inc_part(r);
        Ok(())
    }

    #[tracing::instrument("Flush all", skip_all)]
    pub async fn flush_all(&mut self) -> anyhow::Result<()> {
        for r in 0..self.r {
            tracing::debug!("Flushing {r}");
            self.flush(r)
                .await
                .context(format!("Failed to flush split {r}"))?;
        }
        Ok(())
    }

    #[tracing::instrument("Partitioned file emit", skip_all)]
    pub async fn emit(&mut self, key: &str, value: &str) -> anyhow::Result<()> {
        let r = self.part_func(key) as usize;
        let data = format!("({key}, {value})\n");
        self.flush_or_write(r, data.as_bytes())
            .await
            .context("Failed to flush or write")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::init_tracing;
    use uuid::Uuid;

    #[tokio::test]
    async fn should_emit_r_files_in_bucket_task_id() {
        init_tracing("should emit r files in bucket task test").expect("failed to start tracing");
        // Arrange
        let task_id = Uuid::new_v4();
        let r = 8;

        let s3_client = S3Storage::new(&task_id.to_string())
            .await
            .expect("failed to get storage client");

        // Act
        let mut emitter = PartitionedFileEmitter::new(task_id, r, 2048)
            .await
            .expect("failed to build emitter");

        let mut expected_emitted = vec![];
        for _ in 0..1000 {
            let key = Uuid::new_v4().to_string();
            let value = Uuid::new_v4().to_string();
            emitter.emit(&key, &value).await.expect("failed to emit");
            expected_emitted.push(format!("({key}, {value})"));
        }

        emitter
            .flush_all()
            .await
            .expect("Failed to flush all remaining buffers to storage");

        // Assert
        let expected_emitted_size = expected_emitted.len();
        let mut actual_emitted_size = 0;

        for storage_key in emitter.storage_keys() {
            let result = s3_client.get(storage_key).await.expect("Failed to get key");
            let results: Vec<&str> = result.split("\n").collect();
            for line in results {
                if line.is_empty() {
                    continue;
                }
                assert!(expected_emitted.iter().any(|s| s == line.trim()));
                actual_emitted_size += 1;
            }
        }
        assert_eq!(actual_emitted_size, expected_emitted_size);
    }
}
