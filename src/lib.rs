//! src/lib.rs
pub mod configuration;
pub mod file_splitter;
pub mod mappers;
pub mod mapreduce;
pub mod master;
pub mod reducers;
pub mod spec;
pub mod storage;
pub mod worker;

#[cfg(test)]
mod test_utils;
