//! src/reducers/adder.rs
use crate::reducers::Reducer;
use crate::storage::S3Storage;
use anyhow::Context;

#[derive(Debug)]
pub struct Adder {
    out: Vec<u8>,
    output_location: String,
    r: usize,
}

impl Adder {
    pub fn new(output_location: String, r: usize) -> Self {
        Self {
            out: vec![],
            output_location,
            r,
        }
    }
    pub async fn finish(&self) -> anyhow::Result<()> {
        let output_location = &self.output_location;
        let s3 = S3Storage::new(output_location).await?;
        let key = format!("mr_output_{}", self.r);
        println!("writing to {output_location} with key ={key}");
        s3.put(&key, self.out.as_slice())
            .await
            .context("Failed to write reduce output")?;
        Ok(())
    }
}

impl Reducer for Adder {
    #[tracing::instrument("Adder", skip_all)]
    async fn reduce(
        &mut self,
        key: &str,
        values: Box<dyn Iterator<Item = &str> + '_ + Send + Sync>,
    ) {
        self.emit(&format!("{key}={}\n", &values.count())).await;
    }

    #[tracing::instrument("Emit reduce value", skip_all, fields(value = %value))]
    async fn emit(&mut self, value: &str) {
        self.out.extend(value.as_bytes());
    }
}
