//! Core routing logic for compressed S3 objects.
//!
//! The handler archives source files and dispatches processing based on size:
//! small files -> unzip Lambda, large files -> ECS Fargate task.

use std::{env, io};

use aws_lambda_events::event::s3::S3Event;
use aws_sdk_ecs::{
    types::{
        AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, LaunchType,
        NetworkConfiguration, TaskOverride,
    },
    Client as EcsClient,
};
use aws_sdk_lambda::{primitives::Blob, types::InvocationType, Client as LambdaClient};
use aws_sdk_s3::{types::StorageClass, Client as S3Client};
use lambda_runtime::{tracing, Error, LambdaEvent};
use serde::Serialize;

const DEFAULT_FARGATE_THRESHOLD_MB: u64 = 512;
const DEFAULT_ARCHIVE_PREFIX: &str = "archive";
const DEFAULT_STORAGE_CLASS: &str = "INTELLIGENT_TIERING";

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum FileType {
    Zip,
    Gz,
}

#[derive(Clone)]
struct RouterConfig {
    fargate_threshold_bytes: u64,
    archive_prefix: String,
    archive_storage_class: StorageClass,
    unzip_lambda_function_name: String,
    ecs_cluster_arn: String,
    ecs_task_definition_arn: String,
    ecs_container_name: String,
    ecs_subnets: Vec<String>,
    ecs_security_groups: Vec<String>,
    ecs_assign_public_ip: AssignPublicIp,
}

#[derive(Serialize)]
struct ProcessRequest<'a> {
    bucket: &'a str,
    source_key: &'a str,
    archive_key: &'a str,
    file_type: FileType,
    size_bytes: u64,
}

impl RouterConfig {
    fn from_env() -> Result<Self, Error> {
        let threshold_mb = env::var("FARGATE_THRESHOLD_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_FARGATE_THRESHOLD_MB);

        let archive_prefix = env::var("ARCHIVE_PREFIX")
            .unwrap_or_else(|_| DEFAULT_ARCHIVE_PREFIX.to_string())
            .trim_matches('/')
            .to_string();

        let storage_class_raw =
            env::var("ARCHIVE_STORAGE_CLASS").unwrap_or_else(|_| DEFAULT_STORAGE_CLASS.to_string());
        let archive_storage_class = parse_storage_class(&storage_class_raw);

        let ecs_subnets = split_csv_required("ECS_SUBNETS")?;
        let ecs_security_groups = split_csv_required("ECS_SECURITY_GROUPS")?;

        let ecs_assign_public_ip = match env::var("ECS_ASSIGN_PUBLIC_IP")
            .unwrap_or_else(|_| "false".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "true" | "enabled" => AssignPublicIp::Enabled,
            _ => AssignPublicIp::Disabled,
        };

        Ok(Self {
            fargate_threshold_bytes: threshold_mb.saturating_mul(1024 * 1024),
            archive_prefix,
            archive_storage_class,
            unzip_lambda_function_name: required_env("UNZIP_LAMBDA_FUNCTION_NAME")?,
            ecs_cluster_arn: required_env("ECS_CLUSTER_ARN")?,
            ecs_task_definition_arn: required_env("ECS_TASK_DEFINITION_ARN")?,
            ecs_container_name: required_env("ECS_CONTAINER_NAME")?,
            ecs_subnets,
            ecs_security_groups,
            ecs_assign_public_ip,
        })
    }
}

pub(crate) async fn function_handler(event: LambdaEvent<S3Event>) -> Result<(), Error> {
    let (event, _context) = event.into_parts();

    if event.records.is_empty() {
        tracing::info!("No S3 records in event payload");
        return Ok(());
    }

    let config = RouterConfig::from_env()?;

    let sdk_config = aws_config::load_from_env().await;
    let s3_client = S3Client::new(&sdk_config);
    let lambda_client = LambdaClient::new(&sdk_config);
    let ecs_client = EcsClient::new(&sdk_config);

    for record in event.records {
        let Some(bucket) = record.s3.bucket.name.as_deref() else {
            tracing::warn!("Skipping record with missing bucket name");
            continue;
        };

        let Some(raw_key) = record.s3.object.key.as_deref() else {
            tracing::warn!(bucket = %bucket, "Skipping record with missing object key");
            continue;
        };

        let key = decode_s3_key(raw_key);
        if key.starts_with(&config.archive_prefix) {
            tracing::info!(bucket = %bucket, key = %key, "Skipping already archived object");
            continue;
        }

        let Some(file_type) = detect_file_type(&key) else {
            tracing::info!(bucket = %bucket, key = %key, "Skipping unsupported file extension");
            continue;
        };

        let size_bytes = record.s3.object.size.unwrap_or(0).max(0) as u64;
        tracing::info!(
            bucket = %bucket,
            key = %key,
            size_bytes,
            threshold = config.fargate_threshold_bytes,
            "Routing compressed object"
        );

        let archive_key = archive_object(&s3_client, &config, bucket, &key).await?;

        let request = ProcessRequest {
            bucket,
            source_key: &key,
            archive_key: &archive_key,
           file_type,
            size_bytes,
        };

        if size_bytes > config.fargate_threshold_bytes {
            trigger_fargate(&ecs_client, &config, &request).await?;
            tracing::info!(bucket = %bucket, archive_key = %archive_key, "Dispatched to Fargate");
        } else {
            trigger_lambda(&lambda_client, &config, &request).await?;
            tracing::info!(
                bucket = %bucket,
                archive_key = %archive_key,
                "Dispatched to Lambda unzip function"
            );
        }
    }

    Ok(())
}

async fn archive_object(
    s3_client: &S3Client,
    config: &RouterConfig,
    bucket: &str,
    source_key: &str,
) -> Result<String, Error> {
    let archive_key = format!(
        "{}/{}",
        config.archive_prefix.trim_matches('/'),
        source_key.trim_start_matches('/')
    );
    let copy_source = format!("{}/{}", bucket, urlencoding::encode(source_key));

    s3_client
        .copy_object()
        .bucket(bucket)
        .key(&archive_key)
        .copy_source(copy_source)
        .storage_class(config.archive_storage_class.clone())
        .send()
        .await?;

    s3_client
        .delete_object()
        .bucket(bucket)
        .key(source_key)
        .send()
        .await?;

    Ok(archive_key)
}

async fn trigger_lambda(
    lambda_client: &LambdaClient,
    config: &RouterConfig,
    request: &ProcessRequest<'_>,
) -> Result<(), Error> {
    let payload = serde_json::to_vec(request)?;

    lambda_client
       .invoke()
        .function_name(&config.unzip_lambda_function_name)
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(payload))
        .send()
        .await?;

    Ok(())
}

async fn trigger_fargate(
    ecs_client: &EcsClient,
    config: &RouterConfig,
    request: &ProcessRequest<'_>,
) -> Result<(), Error> {
    let network_config = NetworkConfiguration::builder()
        .awsvpc_configuration(
            AwsVpcConfiguration::builder()
                .set_subnets(Some(config.ecs_subnets.clone()))
                .set_security_groups(Some(config.ecs_security_groups.clone()))
                .assign_public_ip(config.ecs_assign_public_ip.clone())
                .build()?,
        )
        .build();

    let mut env_vars = vec![
        KeyValuePair::builder()
            .name("SOURCE_BUCKET")
            .value(request.bucket)
            .build(),
        KeyValuePair::builder()
            .name("SOURCE_KEY")
            .value(request.source_key)
            .build(),
        KeyValuePair::builder()
            .name("ARCHIVE_KEY")
            .value(request.archive_key)
            .build(),
        KeyValuePair::builder()
            .name("FILE_TYPE")
            .value(match request.file_type {
                FileType::Zip => "zip",
                FileType::Gz => "gz",
            })
           .build(),
    ];
    env_vars.push(
        KeyValuePair::builder()
            .name("SIZE_BYTES")
            .value(request.size_bytes.to_string())
            .build(),
    );

    let overrides = TaskOverride::builder()
        .container_overrides(
            ContainerOverride::builder()
                .name(&config.ecs_container_name)
                .set_environment(Some(env_vars))
                .build(),
        )
        .build();

   ecs_client
        .run_task()
        .cluster(&config.ecs_cluster_arn)
        .task_definition(&config.ecs_task_definition_arn)
        .launch_type(LaunchType::Fargate)
        .network_configuration(network_config)
        .overrides(overrides)
        .send()
       .await?;

    Ok(())
}

fn detect_file_type(key: &str) -> Option<FileType> {
    let lower = key.to_ascii_lowercase();
    if lower.ends_with(".zip") {
        Some(FileType::Zip)
    } else if lower.ends_with(".gz") {
        Some(FileType::Gz)
    } else {
        None
    }
}

fn decode_s3_key(raw_key: &str) -> String {
    match urlencoding::decode(&raw_key.replace('+', "%20")) {
        Ok(decoded) => decoded.into_owned(),
        Err(_) => raw_key.to_string(),
    }
}

fn required_env(name: &str) -> Result<String, Error> {
    env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Missing required environment variable: {name}"),
        )
        .into()
    })
}

fn split_csv_required(name: &str) -> Result<Vec<String>, Error> {
    let value = required_env(name)?;
    let parts: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
       .map(ToOwned::to_owned)
        .collect();

    if parts.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Environment variable {name} must contain at least one value"),
        )
        .into())
    } else {
        Ok(parts)
   }
}

fn parse_storage_class(input: &str) -> StorageClass {
    match input.to_ascii_uppercase().as_str() {
        "STANDARD" => StorageClass::Standard,
        "STANDARD_IA" => StorageClass::StandardIa,
        "ONEZONE_IA" => StorageClass::OnezoneIa,
       "INTELLIGENT_TIERING" => StorageClass::IntelligentTiering,
        "GLACIER" => StorageClass::Glacier,
        "DEEP_ARCHIVE" => StorageClass::DeepArchive,
        "GLACIER_IR" => StorageClass::GlacierIr,
        "REDUCED_REDUNDANCY" => StorageClass::ReducedRedundancy,
        _ => StorageClass::IntelligentTiering,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_runtime::{Context, LambdaEvent};

    #[tokio::test]
    async fn test_event_handler_empty_event() {
        let event = LambdaEvent::new(S3Event::default(), Context::default());
        let response = function_handler(event).await.unwrap();
        assert_eq!((), response);
    }

    #[test]
    fn test_detect_file_type() {
       assert!(matches!(detect_file_type("a/b/c.zip"), Some(FileType::Zip)));
        assert!(matches!(detect_file_type("a/b/c.GZ"), Some(FileType::Gz)));
        assert!(detect_file_type("a/b/c.txt").is_none());
    }

    #[test]
    fn test_decode_s3_key() {
       assert_eq!(decode_s3_key("a%2Fb+c.zip"), "a/b c.zip");
    }
}

