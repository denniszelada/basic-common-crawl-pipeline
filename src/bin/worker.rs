//! The worker(s) pull(s) messages from the RabbitMQ queue and downloads the WARC files that contain the actual content of the URLs.
//! Once the content has been downloaded, the worker extracts the text from the HTML file using the trafilatura Python package.
//!
//! After having downloaded and extracted the text from the HTML file, the worker could apply some filters to the extracted text.
//! We would also want to tokenize (for LLM training) the text and output it to a file.
//!
//! In its current implementation it does not refine or filter the extracted text in any way nor does it output the extracted text to a file.
use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use pipeline::{
    commoncrawl::{download_and_unzip, CdxEntry},
    rabbitmq::{
        rabbitmq_channel_with_queue, rabbitmq_connection, rabbitmq_consumer, CC_QUEUE_NAME,
    },
    tracing_and_metrics::{run_metrics_server, setup_tracing},
    trafilatura,
};
use warc::WarcHeader;
use clap::Parser;
use autometrics::autometrics;
use minio::s3::args::PutObjectArgs;
use minio::s3::client::Client;
use minio::s3::creds::{StaticProvider, Provider};
use minio::s3::http::BaseUrl;
use serde_json::json;
use std::error::Error;
use std::io::Cursor;

#[derive(Parser, Debug)]
struct Args {
    /// The version of the crawl to process, e.g., "CC-MAIN-2024-30".
    #[arg(short='v', long, default_value = "CC-MAIN-2024-30")]
    crawl_version: String,

    /// The name of the MinIO bucket.
    #[arg(long)]
    minio_bucket: String,
}

async fn upload_to_minio(client: &Client, bucket: &str, key: &str, content: &str) -> Result<(), Box<dyn Error>> {
    let mut reader = Cursor::new(content.as_bytes());
    let mut args = PutObjectArgs::new(bucket, key, &mut reader, Some(content.len()), Some("application/json".len()))?;
    client.put_object(&mut args).await?;
    Ok(())
}

#[autometrics]
async fn process_batch(batch: Vec<CdxEntry>, args: &Args, client: &Client) {
    for entry in batch {
        let data = download_and_unzip(
            &format!(
                "https://data.commoncrawl.org/{}/{}",
                args.crawl_version,
                entry.metadata.filename
            ),
            entry.metadata.offset,
            entry.metadata.length,
        )
        .await
        .unwrap();
        for warc_entry in warc::WarcReader::new(data.as_slice()).iter_records() {
            let warc_entry = warc_entry.unwrap();
            if warc_entry.header(WarcHeader::WarcType).unwrap() != "response" {
                continue;
            }
            tracing::info!(
                "Successfully read WARC entry with URL {}",
                warc_entry.header(WarcHeader::TargetURI).unwrap()
            );
            let raw_content = String::from_utf8_lossy(warc_entry.body());
            let html_begin_index = raw_content.find("\n\n");
            let Some(html_begin_index) = html_begin_index else {
                tracing::warn!("Failed to find HTML content in WARC entry");
                continue;
            };
            let content = trafilatura::extract(&raw_content[html_begin_index..]).unwrap();
            if let Some(content) = content {
                let json_content = json!({
                    "url": entry.metadata.url,
                    "content": content
                });
                let key = format!("{}.json", entry.metadata.url.replace("/", "_"));
                upload_to_minio(client, &args.minio_bucket, &key, &json_content.to_string()).await.unwrap();
                tracing::info!("Uploaded content to MinIO with key {}", key);
            } else {
                tracing::warn!("Failed to extract content from WARC entry");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_tracing();
    tokio::task::spawn(run_metrics_server(9001));

    let base_url = "http://localhost:9000".parse::<BaseUrl>().unwrap();
    let provider: Option<Box<dyn Provider + Send + Sync>> = Some(Box::new(StaticProvider::new("your-access-key", "your-secret-key", None)));
    let client = Client::new(base_url, provider, None, None).unwrap();

    let rabbit_conn = rabbitmq_connection().await.unwrap();
    let (channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME)
        .await
        .unwrap();
    let mut consumer = rabbitmq_consumer(&channel, CC_QUEUE_NAME, "worker")
        .await
        .unwrap();
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let batch = serde_json::from_slice::<Vec<CdxEntry>>(&delivery.data);
                tracing::info!(
                    "Received a batch of {} entries",
                    batch.as_ref().unwrap().len()
                );
                process_batch(batch.unwrap(), &args, &client).await;
                delivery.ack(BasicAckOptions::default()).await.unwrap();
            }
            Err(e) => {
                tracing::warn!(err.msg = %e, err.details = ?e, "Failed to receive message from RabbitMQ. Reconnecting.");
                continue;
            }
        }
    }
}
