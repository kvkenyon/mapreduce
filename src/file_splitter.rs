//! src/file_splitter.rs
use anyhow::Context;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

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

        let in_file = File::open(&self.path).context("Failed to open input file")?;

        let buf_reader = BufReader::new(&in_file);
        let mut results: Vec<PathBuf> = Vec::new();
        let (mut curr_out_file, mut path) = self.create_new_out_file("0")?;
        let mut curr_out_file_size = 0;
        let mut curr_suffix = 0;

        let mut it = buf_reader.lines().peekable();
        while let Some(Ok(line)) = it.next() {
            let line_size = line.len();

            if line_size + curr_out_file_size > self.split_size_in_bytes as usize {
                results.push(path);
                // Create next file and reset state
                curr_suffix += 1;
                (curr_out_file, path) = self.create_new_out_file(&curr_suffix.to_string())?;
                curr_out_file_size = 0;
            }

            curr_out_file
                .write(line.as_bytes())
                .context("Failed to write line to out file")?;
            curr_out_file_size += line_size;
            // Ensure we return the last file path in the results
            if it.peek().is_none() {
                results.push(path);
                break;
            }
        }
        Ok(results)
    }

    fn create_new_out_file(&self, suffix: &str) -> Result<(File, PathBuf), anyhow::Error> {
        let name = format!("{}_{suffix}", self.out_file_name);
        let mut path = PathBuf::from(&self.out_path);
        path.push(name);
        let file = File::create(&path).context(format!(
            "Failed to create file at: {}",
            path.to_str().context("Failed to parse file path")?
        ))?;
        Ok((file, path))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::test_utils::test_data_dir;
    use claims::{assert_err, assert_le};

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

        let file_size = fs::metadata(&filename)
            .expect("Failed to read metadata")
            .len();

        assert_eq!(file_size, 2633);

        let out_path = PathBuf::from(format!("/tmp/mapreduce/{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&out_path).expect("Failed to create test directory");

        let splitter = FileSplitter::new(filename, 100, "test_out".to_string(), out_path.clone());

        let out_files = splitter.split().expect("Failed to split");

        for (i, path) in out_files.iter().enumerate() {
            let name = format!("test_out_{i}");
            assert_eq!(path.file_name().unwrap().to_str().unwrap(), name);
        }

        fs::remove_dir_all(out_path).expect("Failed to delete dirs");
    }

    #[test]
    fn should_write_the_correct_amount_of_data_to_outfiles() {
        let mut filename = test_data_dir();
        filename.push("small_test.txt");

        let file_size = fs::metadata(&filename)
            .expect("Failed to read metadata")
            .len();

        assert_eq!(file_size, 2633);

        let out_path = PathBuf::from(format!("/tmp/mapreduce/{}", uuid::Uuid::new_v4()));

        fs::create_dir_all(&out_path).expect("Failed to create test directory");

        let splitter = FileSplitter::new(filename, 100, "test_out".to_string(), out_path.clone());
        let out_files = splitter.split().expect("Failed to split");

        for path in out_files {
            let size = fs::metadata(path).unwrap().len();
            assert_le!(size, 100);
        }
        fs::remove_dir_all(out_path).expect("Failed to delete dirs");
    }
}
