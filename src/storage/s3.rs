//! src/storage/s3.rs
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::Config;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use secrecy::ExposeSecret;

use crate::configuration::get_configuration;

#[derive(Debug)]
pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    pub async fn new(bucket_name: &str) -> Result<Self, anyhow::Error> {
        let config = get_configuration().expect("Failed to get configuration");
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
                println!("Successfully created bucket: {}", bucket_name);
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

    pub async fn get(&self, key: &str) -> Result<String, anyhow::Error> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        let data = result.body.collect().await?.into_bytes();
        let response = std::str::from_utf8(&data)?;
        Ok(response.to_string())
    }

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
