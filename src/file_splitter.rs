//! src/file_splitter.rs
use std::fs::{self, File};
use std::{os::unix::fs::MetadataExt, path::PathBuf};

use anyhow::Context;

#[allow(unused)]
pub struct FileSplitter {
    split_size_in_bytes: u64,
    path: PathBuf,
    out_path: PathBuf,
    out_file_name: String,
}

impl FileSplitter {
    pub fn new(
        path: PathBuf,
        split_size_in_bytes: u64,
        out_file_name: String,
        out_path: PathBuf,
    ) -> Self {
        FileSplitter {
            split_size_in_bytes,
            path,
            out_file_name,
            out_path,
        }
    }

    pub fn split(&self) -> Result<Vec<PathBuf>, anyhow::Error> {
        if !self.path.exists() {
            return Err(anyhow::anyhow!(format!(
                "Input file doesn't exist: {}",
                self.path.to_str().unwrap()
            )));
        }

        let metadata = self.path.metadata().context("Failed to extract metadata")?;
        let size_in_bytes = metadata.size();

        let mut out_file_count = size_in_bytes / self.split_size_in_bytes;
        if size_in_bytes % self.split_size_in_bytes != 0 {
            out_file_count += 1;
        }

        let _in_file = fs::read_to_string(&self.path).context("Failed to read input file")?;
        let mut results: Vec<PathBuf> = Vec::with_capacity(out_file_count as usize);
        for i in 0..out_file_count {
            let mut result = PathBuf::from(&self.out_path);
            result.push(format!("{}_{i}", self.out_file_name));

            let _file = File::create(&result).context(format!(
                "Failed to create file at: {}",
                result.to_str().context("Failed to parse file path")?
            ))?;

            results.push(result);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::test_utils::test_data_dir;
    use claims::assert_err;

    #[test]
    fn should_fail_if_in_file_doesnt_exist() {
        let mut filename = test_data_dir();
        filename.push("non_existent_file.txt");
        let splitter = FileSplitter::new(
            filename,
            1000,
            "test_out".to_string(),
            PathBuf::from("/tmp/mapreduce"),
        );
        assert_err!(splitter.split());
    }

    #[test]
    fn should_fail_gracefully_if_out_file_path_is_empty() {
        let mut filename = test_data_dir();
        filename.push("non_existent_file.txt");
        let splitter = FileSplitter::new(filename, 1000, "test_out".to_string(), PathBuf::new());
        assert_err!(splitter.split());
    }

    #[test]
    fn should_store_the_files_at_out_path_with_out_file_name_with_index() {
        let mut filename = test_data_dir();
        filename.push("small_test.txt");

        let file_size = filename.metadata().unwrap().size();

        assert_eq!(file_size, 2633);

        let out_path = PathBuf::from("/tmp/mapreduce");

        let splitter = FileSplitter::new(filename, 100, "test_out".to_string(), out_path);

        let out_files = splitter.split().expect("Failed to split");

        assert_eq!(out_files.len(), 27);

        for file in out_files {
            assert!(file.exists());
            fs::remove_file(file).expect("Failed to remove file");
        }
    }
}
