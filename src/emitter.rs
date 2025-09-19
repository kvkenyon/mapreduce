//! src/emitter.rs

// Given a number of reduce tasks R the file based emitter should use the input key
// and pass it to a part_func(k) % R returning r. Then emit all outputs to a local
// buffer. This buffer gets cleared periodically to
// s3://<bucket_name>/<task-id>/mr_input_{r}.{txt/json/etc.}
// Define: Periodically: every 3 seconds
// What happens on crash? Master reschedules the task on a new worker

// Stuff to do:
// We need to provide the bucket name to each worker
// We need a way to register Mappers and Reducers
// We need a way to bind the task id to the execution of a task

/*
Maybe we just make one emitter right now that writes to S3 regardless.
And then we can build executors that take a map/reduce task and
execute it for the worker. This way we can bind the task id
to the file emitter. And a map function just call the global
emit function, and same for reduce.

*/
use crate::storage::S3Storage;
use anyhow::Context;
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use uuid::Uuid;

pub fn map_emit(key: &str, value: &str) {
    tracing::info!("({key}, {value})");
}

pub fn reduce_emit(value: &str) {
    tracing::info!("{value}");
}

// Given a number of reduce tasks R the file based emitter should use the input key
// and pass it to a part_func(k) % R returning r. Then emit all outputs to a local
// buffer. This buffer gets cleared periodically to
// s3://<bucket_name>/<task-id>/r/mr_input.{txt/json/etc/.}
// Define: Periodically: every 3 seconds
// What happens on crash? Master reschedules the task on a new worker

// Stuff to do:
// We need to provide the bucket name to each worker
// We need a way to register Mappers and Reducers
// We need a way to bind the task id to the execution of a task

/*
Maybe we just make one emitter right now that writes to S3 regardless.
And then we can build executors that take a map/reduce task and
execute it for the worker. This way we can bind the task id
to the file emitter. And a map function just call the global
emit function, and same for reduce.

We can either build an AsyncS3Writer that uses multipart upload or
just write the intermediate outputs into separate files...there will be a lot of files
unless we make our buffer large....then the master can either consolidate and pass the file uri
to the reduce worker or just let the reduce worker consolidate.

*/

pub struct PartitionedFileEmitter {
    bucket_name: String,
    task_id: Uuid,
    r: usize,
    parts: Vec<usize>,
    buf_writers: Vec<BufWriter<File>>,
}

impl PartitionedFileEmitter {
    pub fn new(bucket_name: &str, task_id: Uuid, r: usize) -> anyhow::Result<Self> {
        let mut buf_writers = Vec::with_capacity(r);
        let path = format!("/tmp/mapreduce/{task_id}/");
        std::fs::create_dir_all(&path).context("Failed to create intermediate file directory")?;
        let mut path = PathBuf::from(path);
        for _ in 0..r {
            path.push(format!("map_input_{r}.txt"));
            let file = File::create(&path).context("Failed to create intermediate output file")?;
            let buf_writer = BufWriter::with_capacity(100, file);
            buf_writers.push(buf_writer);
            path.pop();
        }
        let mut parts = Vec::with_capacity(r);
        parts.fill(0);
        Ok(Self {
            bucket_name: bucket_name.to_string(),
            task_id,
            r,
            parts,
            buf_writers,
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
        self.parts.insert(r, *curr_part + 1usize);
    }

    async fn flush_or_write(&mut self, r: usize, data: &[u8]) -> Result<(), anyhow::Error> {
        let bucket_name: &str = &self.bucket_name.clone();
        let part = self.parts.get(r).unwrap();
        let key = format!("/{}/{r}/{}/mr_input.txt", self.task_id, part);
        let file_path = PathBuf::from(format!("/tmp/mapreduce/{}/mr_input_{r}", self.task_id));
        let buf_size = data.len();

        let buf_writer = self.get_buf_writer(r);
        if buf_writer.buffer().len() + data.len() > buf_size {
            let s3 = S3Storage::new(bucket_name)
                .await
                .context("Failed to acquire s3 storage handle")?;
            buf_writer.flush().context("Failed to flush writer.")?;
            let mut file = File::open(file_path).context("Failed to open file.")?;
            let mut buf: Vec<u8> = vec![];
            file.read(&mut buf)
                .context("Failed to read file into buf")?;
            s3.put(&key, &buf).await.context("Failed to write to s3")?;
            self.inc_part(r);
        } else {
            buf_writer
                .write_all(data)
                .context("Failed to write to buf writer")?;
        }
        Ok(())
    }

    #[tracing::instrument("Partitioned file emit", skip_all)]
    pub async fn emit(&mut self, key: &str, value: &str) -> anyhow::Result<()> {
        let r = self.part_func(key) as usize;
        let data = format!("({key}, {value}");
        self.flush_or_write(r, data.as_bytes())
            .await
            .context("Failed to flush or write")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn should_emit_r_files_in_bucket_task_id() {}
}
