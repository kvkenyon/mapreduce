//! src/lib.rs
pub mod configuration;
pub mod executors;
pub mod file_splitter;
pub mod functions;
pub mod mapreduce;
pub mod master;
pub mod spec;
pub mod util;
pub mod worker;

#[cfg(test)]
mod test_utils;
