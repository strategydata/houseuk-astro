//! Experimental Rust prototype for crime data download and archive handling.

use async_zip::base::read::seek::ZipFileReader;
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tempfile::Builder;
use reqwest::Result;

async fn download_files(url: &str, path: &str) -> Result<()>  {
    let mut file = File::create(path).await?;
    println!("Downloading {}...", url);

    let mut stream = reqwest::get(url)
        .await?
        .bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        file.write_all(&chunk).await?;
    }

    file.flush().await?;

    println!("Downloaded {}", url);
    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    let url = "https://data.police.uk/data/archive/2025-12.zip";
    let bucket = "quibbler-house-data-lake";
    let zip_path = "/tmp/data.zip";
    download_files(url, zip_path).await?;
    println!("Successfully");
    // AWS Client Setup
    // let config = aws_config::load_from_env().await;
    // let s3_client = s3::Client::new(&config);

    // // 1. STREAM DOWNLOAD TO DISK (Save RAM)
    // println!("Downloading ZIP...");
    // let mut response = reqwest::get(url).await?;
    // let mut file = File::create(zip_path).await?;
    
    // while let Some(chunk) = response.chunk().await? {
    //     file.write_all(&chunk).await?;
    // }
    // file.flush().await?;

    // // 2. UPLOAD RAW ZIP TO S3
    // println!("Uploading raw ZIP...");
    // s3_client.put_object()
    //     .bucket(bucket)
    //     .key("raw/source=datafeed/year=2026/month=02/data.zip")
    //     .body(ByteStream::from_path(Path::new(zip_path)).await?)
    //     .send()
    //     .await?;

    // // 3. UNZIP AND UPLOAD INDIVIDUAL FILES
    // println!("Extracting and uploading files...");
    // let file = File::open(zip_path).await?;
    // let mut reader = ZipFileReader::with_tokio(file).await?;
    
    // let num_entries = reader.file().entries().len();
    // for i in 0..num_entries {
    //     let entry = &reader.file().entries()[i];
    //     let filename = entry.filename().as_str()?.to_string();

    //     // Skip directories
    //     if filename.ends_with('/') { continue; }

    //     // Read entry content into a buffer
    //     let mut entry_reader = reader.reader_without_entry(i).await?;
    //     let mut buffer = Vec::new();
    //     entry_reader.read_to_end_checked(&mut buffer).await?;

    //     // Upload to S3
    //     let s3_key = format!("processed/source=datafeed/year=2026/month=02/{}", filename);
    //     s3_client.put_object()
    //         .bucket(bucket)
    //         .key(s3_key)
    //         .body(ByteStream::from(buffer))
    //         .send()
    //         .await?;
        
    //     println!("Uploaded: {}", filename);
    // }

    // // 4. CLEANUP
    // fs::remove_file(zip_path).await?;
    // println!("Done!");

    Ok(())
}

