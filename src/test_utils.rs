//! src/test_utils.rs
#[cfg(test)]
use std::path::PathBuf;

pub fn test_data_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path
}
