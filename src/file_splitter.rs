//! src/file_splitter.rs
use crate::storage::S3Storage;
use anyhow::Context;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

#[allow(unused)]
#[derive(Debug)]
pub struct AsyncFileSplitter {
    bucket_name: String,
    input_key: String,
    output_key: String,
    split_size_in_bytes: u32,
    tmp_dir: PathBuf,
}

impl AsyncFileSplitter {
    pub fn new(
        job_id: &str,
        bucket_name: &str,
        input_key: &str,
        output_key: &str,
        split_size_in_bytes: u32,
    ) -> Result<Self, anyhow::Error> {
        println!(
            "new async splitter for input_key={input_key} and output_key={output_key} with split_size_in_bytes={split_size_in_bytes}"
        );
        let path = PathBuf::from(format!("/tmp/mapreduce/jobs/{job_id}"));
        std::fs::create_dir_all(&path).context(format!(
            "Failed to create local temp directory at: {}",
            path.to_str().unwrap()
        ))?;
        Ok(Self {
            bucket_name: bucket_name.into(),
            input_key: input_key.into(),
            output_key: output_key.into(),
            split_size_in_bytes,
            tmp_dir: path,
        })
    }

    pub async fn split(&mut self) -> Result<Vec<String>, anyhow::Error> {
        fn result_key(output_key: &str, curr_suffix: u8) -> String {
            format!("{output_key}_{curr_suffix}")
        }

        let s3 = S3Storage::new(&self.bucket_name)
            .await
            .context("Failed to get storage handle")?;

        self.tmp_dir.push(&self.input_key);
        s3.get_to_file(&self.input_key, &self.tmp_dir).await?;

        let in_file = File::open(&self.tmp_dir).context("Failed to get in file")?;
        let buf_reader = BufReader::new(&in_file);
        let mut out_buff: Vec<u8> = vec![];
        let mut curr_suffix = 0u8;
        let mut it = buf_reader.lines().peekable();
        let mut results = vec![];
        while let Some(Ok(mut line)) = it.next() {
            line.push('\n');
            let line_size = line.len();
            if line_size + out_buff.len() > self.split_size_in_bytes as usize {
                s3.put(
                    &result_key(&self.output_key, curr_suffix),
                    out_buff.as_slice(),
                )
                .await?;
                results.push(result_key(&self.output_key, curr_suffix));
                // Create empty buffer
                curr_suffix += 1;
                out_buff = vec![];
            }
            out_buff.extend_from_slice(line.as_bytes());
            // Ensure we put the last file to storage
            if it.peek().is_none() {
                s3.put(
                    &result_key(&self.output_key, curr_suffix),
                    out_buff.as_slice(),
                )
                .await?;
                results.push(result_key(&self.output_key, curr_suffix));
                break;
            }
        }

        self.tmp_dir.pop();
        Ok(results)
    }
}
