//! src/storage/s3.rs
use crate::configuration::get_configuration;
use anyhow::Context;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::Config;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use secrecy::ExposeSecret;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug)]
pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    #[tracing::instrument(name = "Create S3Storage handle")]
    pub async fn new(bucket_name: &str) -> Result<Self, anyhow::Error> {
        let config = get_configuration().context("Failed to get configuration")?;
        let creds = Credentials::new(
            config.storage.aws_access_key_id,
            config.storage.aws_secret_key.expose_secret(),
            None,
            None,
            "mapreduce",
        );

        let config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(creds)
            .region(Region::new(config.storage.aws_region))
            .endpoint_url(config.storage.aws_endpoint_url)
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(config);
        let create_result = client.create_bucket().bucket(bucket_name).send().await;

        match create_result {
            Ok(_) => {
                tracing::debug!("Successfully acquired handle to bucket: {}", bucket_name);
            }
            Err(err) => {
                let service_error = err.as_service_error();
                if let Some(e) = service_error {
                    // Check if bucket already exists
                    if e.is_bucket_already_exists() || e.is_bucket_already_owned_by_you() {
                        return Err(anyhow::anyhow!("Bucket {} already exists", bucket_name));
                    }
                }
                return Err(anyhow::anyhow!("Failed to create bucket: {}", err));
            }
        }

        Ok(Self {
            client,
            bucket: bucket_name.to_string(),
        })
    }

    #[tracing::instrument(name = "Put", skip(data))]
    pub async fn put(&self, key: &str, data: &[u8]) -> Result<(), anyhow::Error> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data.to_vec()))
            .send()
            .await?;
        Ok(())
    }

    #[tracing::instrument(name = "Get string")]
    pub async fn get(&self, key: &str) -> Result<String, anyhow::Error> {
        let data = self
            .get_stream(key)
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        let response = std::str::from_utf8(&data)?;
        Ok(response.to_string())
    }

    #[tracing::instrument(name = "Get to file", fields(path = path.to_str().unwrap_or_else(|| "n/a")))]
    pub async fn get_to_file(&self, key: &str, path: &PathBuf) -> Result<File, anyhow::Error> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .context(format!(
                "Failed to create file for S3 download: {}",
                path.to_str().unwrap()
            ))?;

        let mut object = self.get_stream(key).await?;
        while let Some(bytes) = object
            .body
            .try_next()
            .await
            .context("Failed to read from S3 download stream")?
        {
            file.write_all(&bytes).map_err(|err| {
                anyhow::anyhow!(format!(
                    "Failed to write from S3 download stream to local file: {err:?}"
                ))
            })?;
        }
        Ok(file)
    }

    #[tracing::instrument(name = "Get stream")]
    pub async fn get_stream(&self, key: &str) -> Result<GetObjectOutput, anyhow::Error> {
        self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context("Failed to get object output stream")
    }

    #[tracing::instrument(name = "List")]
    pub async fn list(&self) -> Result<Vec<String>, anyhow::Error> {
        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .send()
            .await?;

        let keys = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(String::from))
            .collect();
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::S3Storage;
    use crate::test_utils::test_data_dir;
    use std::fs;
    use uuid::Uuid;

    #[tokio::test]
    async fn should_be_able_to_list_bucket_objects() {
        let bucket = Uuid::new_v4();
        let storage = S3Storage::new(&bucket.to_string())
            .await
            .expect("Failed to create S3 storage bucket");
        let objects = storage.list().await.expect("Failed to get objects");
        assert!(objects.is_empty());
    }

    #[tokio::test]
    async fn should_be_able_to_save_objects_and_get_them_back() {
        let bucket = Uuid::new_v4();
        let storage = S3Storage::new(&bucket.to_string())
            .await
            .expect("Failed to create S3 storage bucket");

        let job_id = Uuid::new_v4().to_string();

        let mut path = test_data_dir();
        path.push("small_test.txt");
        let data = fs::read_to_string(path).expect("Failed to read test file");

        let key = format!("/tmp/mapreduce/jobs/{}/input_split.txt", job_id);

        storage
            .put(&key, &data.clone().into_bytes())
            .await
            .expect("Failed to store data in bucket");

        let objects = storage.list().await.expect("Failed to get objects");

        assert!(!objects.is_empty());
        assert!(objects.contains(&key));

        let result = storage.get(&key).await.expect("Failed to get object");

        assert_eq!(data, result);
    }
}
