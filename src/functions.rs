//! src/functions.rs

use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::{collections::HashMap, path::PathBuf};

pub type Key = String;
pub type Value = String;

pub struct MapInput {
    key: Key,
    value: Value,
}

impl MapInput {
    pub fn new(key: Key, value: Value) -> Self {
        MapInput { key, value }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

pub struct ReduceInput<I: Iterator<Item = Value>> {
    key: Key,
    values: I,
}

impl<I: Iterator<Item = Value>> ReduceInput<I> {
    pub fn new(key: Key, values: I) -> Self {
        ReduceInput { key, values }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn into_values(self) -> I {
        self.values
    }
}

pub trait MapEmitter {
    fn emit(&mut self, key: Key, value: Value);
}

pub trait Mapper {
    type Emitter: MapEmitter;
    fn build(emitter: Self::Emitter) -> Self;
    fn map(&mut self, key: Key, value: Value);
}

pub trait ReduceEmitter {
    fn emit(&self, value: Value);
}

pub trait Reducer {
    type Emitter: ReduceEmitter;
    fn build(emitter: Self::Emitter) -> Self;
    fn reduce<I: Iterator<Item = Value>>(&self, key: Key, values: I);
}

pub struct DefaultMapEmitter;
pub struct DefaultReduceEmitter;

// Add these implementations
impl Default for DefaultMapEmitter {
    fn default() -> Self {
        DefaultMapEmitter
    }
}

impl Default for DefaultReduceEmitter {
    fn default() -> Self {
        DefaultReduceEmitter
    }
}

impl MapEmitter for DefaultMapEmitter {
    fn emit(&mut self, key: Key, value: Value) {
        println!("({key}, {value})");
    }
}

impl ReduceEmitter for DefaultReduceEmitter {
    fn emit(&self, value: Value) {
        println!("({value})");
    }
}

pub struct FileMapEmitter {
    output_dir: PathBuf,
    partition_count: usize,
    writers: HashMap<String, BufWriter<File>>,
}

impl FileMapEmitter {
    pub fn new(output_dir: impl AsRef<Path>, partition_count: usize) -> std::io::Result<Self> {
        let output_dir = output_dir.as_ref().to_path_buf();
        create_dir_all(&output_dir)?;
        Ok(Self {
            output_dir,
            partition_count,
            writers: HashMap::new(),
        })
    }

    pub fn get_partition(&self, key: &str) -> usize {
        let mut hash = 0usize;

        for byte in key.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as usize);
        }

        hash % self.partition_count
    }

    // Get or create a writer for a specific partition
    fn get_writer(&mut self, partition: usize) -> std::io::Result<&mut BufWriter<File>> {
        let partition_key = format!("R{:04}", partition);

        // Use entry API to get or create writer
        let output_dir = self.output_dir.clone();
        let writer = self
            .writers
            .entry(partition_key.clone())
            .or_insert_with(|| {
                let file_path = output_dir.join(format!("{}.txt", partition_key));
                let file = File::create(file_path).expect("Failed to create partition file");
                BufWriter::with_capacity(8192, file) // 8KB buffer
            });

        Ok(writer)
    }

    // Flush all writers
    pub fn flush_all(&mut self) -> std::io::Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    // Close all writers (automatically flushes)
    pub fn close(mut self) -> std::io::Result<()> {
        self.flush_all()?;
        // Writers are dropped here, which also flushes them
        Ok(())
    }
}

impl MapEmitter for FileMapEmitter {
    fn emit(&mut self, key: Key, value: Value) {
        let partition = self.get_partition(&key);

        match self.get_writer(partition) {
            Ok(writer) => {
                // Write as tab-separated values (or any format you prefer)
                writeln!(writer, "{}\t{}", key, value).expect("Failed to write to partition file");
            }
            Err(e) => {
                eprintln!("Error writing to partition {}: {}", partition, e);
            }
        }
    }
}

// For automatic cleanup
impl Drop for FileMapEmitter {
    fn drop(&mut self) {
        let _ = self.flush_all();
    }
}

// Make it work with Default for the registry
impl Default for FileMapEmitter {
    fn default() -> Self {
        Self::new("/tmp/mapreduce/output/map", 10).expect("Failed to create default FileMapEmitter")
    }
}
