use lambda_runtime::{tracing, Error, LambdaEvent};
use aws_lambda_events::event::s3::S3Event;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
///
const SIZE_THRESHOLD: i64 = 500 * 1024 * 1024; // 10 MB
pub(crate)async fn function_handler(event: LambdaEvent<S3Event>) -> Result<(), Error> {
    // Extract some useful information from the request
    let (event, _context) = event.into_parts();
    for record in event.records {
        let bucket = record.s3.bucket.name.as_ref().unwrap();
        let key = record.s3.object.key.as_ref().unwrap();
        let size = record.s3.object.size.unwrap_or(0);
        tracing::info!(bucket = %bucket, file_key = %key, file_size = size, "Evaluating routing path");

        if size > SIZE_THRESHOLD {
            tracing::info!("File size exceeds threshold, routing to slow path");
            // Add your logic for handling large files here
        } else {
            tracing::info!("File size is within threshold, routing to fast path");
            // Add your logic for handling small files here
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_runtime::{Context, LambdaEvent};

    #[tokio::test]
    async fn test_event_handler() {
        let event = LambdaEvent::new(S3Event::default(), Context::default());
        let response = function_handler(event).await.unwrap();
        assert_eq!((), response);
    }
}
