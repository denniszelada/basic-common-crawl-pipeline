//! The batcher only operates on index files that contain metadata about the URLs that are part of the crawl.
//! It does not have to download the actual content of the URLs and therefore it does not have to deal with WARC files.
//!
//! For a given crawl, there are hundreds of index files, each containing roughly a gigabyte of URL metadata.
//! Every line in the index file contains the following information. Notice that I have split the line into multiple lines for readability:
//!
//! ```json
//! 0,100,22,165)/
//! 20240722120756
//! {
//!     "url": "http://165.22.100.0/",
//!     "mime": "text/html",
//!     "mime-detected": "text/html",
//!     "status": "301",
//!     "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R",
//!     "length": "689",
//!     "offset": "3499",
//!     "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz",
//!     "redirect": "https://157.245.55.71/"
//! }
//! ```
//!
//! The first lines contains the URL in SURT (Sort-friendly URI Reordering Transform) format, the second lines contains the crawl timestamp, and the remaining lines contain JSON metadata.
//!
//! The URLs in the index files are sorted alpha-numerically.
//!
//! Once the batcher has downloaded (parts of) an index file, it will filter out URLs that are not in English or that did not return a 200 HTTP status code, batch them into groups whose size has a constant upper limit and push the messages containing these URls into a RabbitMQ queue.
use clap::Parser;
use pipeline::commoncrawl::{parse_cdx_line, parse_cluster_idx, ClusterIdxEntry, CdxEntry, download_and_unzip};
use pipeline::rabbitmq::{rabbitmq_channel_with_queue, rabbitmq_connection, BATCH_SIZE, CC_QUEUE_NAME};
use pipeline::tracing_and_metrics::{run_metrics_server, setup_tracing};
use autometrics::autometrics;
use std::fs;
use lapin::Channel;
use serde_json::to_string;
use std::path::Path;
use std::env;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// For an explanation for why this file needs to be provided, please
    /// see Readme.md, section "Why do we download the cluster.idx file up front?".
    #[arg(short, long, default_value = "cluster.idx")]
    cluster_idx_filename: String,

    /// This command line argument can be used to limit the number of chunks that should be processed.
    /// If set, the batcher only processes so many lines from the provided cluster.idx file.
    /// Otherwise, it processes all entries in the file.
    #[arg(short, long)]
    num_cdx_chunks_to_process: Option<usize>,

    /// The version of the crawl to process, e.g., "CC-MAIN-2024-30".
    #[arg(short='v', long, default_value = "CC-MAIN-2024-30")]
    crawl_version: String,
}

#[autometrics]
async fn process_cdx_chunk(cdx_chunk: ClusterIdxEntry, args: &Args, channel: &Channel) -> usize {
    let batch_size = BATCH_SIZE;
    let mut num_batches = 0;
    let mut current_batch = Vec::with_capacity(batch_size);

    // Download and unzip the CDX file
    let cdx_content = download_and_unzip(
        &format!(
            "https://data.commoncrawl.org/cc-index/collections/{}/indexes/{}",
            args.crawl_version,
            cdx_chunk.cdx_filename
        ),
        cdx_chunk.cdx_offset,
        cdx_chunk.cdx_length,
    )
    .await
    .expect("Failed to download and unzip CDX file");

    // Process the downloaded content
    String::from_utf8_lossy(&cdx_content)
        .lines()
        .filter_map(|line| {
            let entry = parse_cdx_line(line);
            Some(entry)
        })
        .filter(|e| {
            if let Some(languages) = e.metadata.languages.as_ref() {
                languages.contains("eng") && e.metadata.status == 200
            } else {
                false
            }
        })
        .for_each(|entry| {
            current_batch.push(entry);
            if current_batch.len() == batch_size {
                let channel = channel.clone();
                let batch = current_batch.clone();
                tokio::spawn(async move {
                    publish_batch_local(&channel, CC_QUEUE_NAME, &batch).await;
                });
                current_batch.clear();
                num_batches += 1;
            }
        });

    // Send any remaining entries in the last batch
    if !current_batch.is_empty() {
        publish_batch_local(channel, CC_QUEUE_NAME, &current_batch).await;
        num_batches += 1;
    }

    num_batches
}

async fn publish_batch_local(channel: &Channel, queue_name: &str, batch: &[CdxEntry]) {
    let payload = to_string(batch).expect("Failed to serialize batch");
    channel
        .basic_publish(
            "",
            queue_name,
            lapin::options::BasicPublishOptions::default(),
            payload.as_bytes(),
            lapin::BasicProperties::default(),
        )
        .await
        .expect("Failed to publish batch");
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_tracing();
    tokio::task::spawn(run_metrics_server(9000));

    let rabbit_conn = rabbitmq_connection().await.unwrap();
    let (channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME)
        .await
        .unwrap();

    // Print the current working directory
    if let Ok(current_dir) = env::current_dir() {
        println!("Current working directory: {:?}", current_dir);
    }

    // Print the cluster index filename
    println!("Cluster index filename: {:?}", args.cluster_idx_filename);

    let cluster_idx_path = Path::new(&args.cluster_idx_filename);
    println!("Looking for file: {:?}", cluster_idx_path);

    if !cluster_idx_path.exists() {
        eprintln!("Error: The file '{}' does not exist.", args.cluster_idx_filename);
        std::process::exit(1);
    }

    let idx = fs::read_to_string(cluster_idx_path)
        .expect("Should have been able to read the file")
        .lines()
        .filter_map(|line| parse_cluster_idx(line))
        .collect::<Vec<_>>();

    let mut num_cdx_chunks_processed: usize = 0;
    for cdx_chunk in idx {
        print!(".");
        num_cdx_chunks_processed += process_cdx_chunk(cdx_chunk, &args, &channel).await;
        if let Some(to_process) = args.num_cdx_chunks_to_process {
            if to_process == num_cdx_chunks_processed {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pipeline::commoncrawl::{parse_cdx_line, parse_cluster_idx};


    #[test]
    fn can_parse_cdx_file_with_three_lines() {
        let content = r#"0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}
0,100,22,165)/robots.txt 20240722120755 {"url": "http://165.22.100.0/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "LYEE2BXON4MCQCP5FDVDNILOWBKCZZ6G", "length": "700", "offset": "4656", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/robotstxt/CC-MAIN-20240722095039-20240722125039-00410.warc.gz", "redirect": "https://157.245.55.71/robots.txt"}
0,100,59,139)/ 20240723213521 {"url": "https://139.59.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "5JOQMMSNM6N7UCLGGYXDSPSB3FYAQS2C", "length": "16650", "offset": "64016172", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763518115.82/warc/CC-MAIN-20240723194208-20240723224208-00279.warc.gz", "charset": "UTF-8", "languages": "ind,eng"}"#;
        let cdx: Vec<_> = content.lines().map(parse_cdx_line).collect();
        assert_eq!(cdx.len(), 3);
    }

    #[test]
    fn can_parse_cluster_idx_file_with_four_lines() {
        let content = r#"0,100,22,165)/ 20240722120756   cdx-00000.gz    0       188224  1
101,141,199,66)/robots.txt 20240714155331       cdx-00000.gz    188224  178351  2
104,223,1,100)/ 20240714230020  cdx-00000.gz    366575  178055  3
107,128,254,23)/sites.asp?domain=hydrogenheaters.com 20240725183414     cdx-00000.gz    544630  181599  4"#;
        let cdx_parts: Vec<_> = content.lines().map(parse_cluster_idx).collect();
        assert_eq!(cdx_parts.len(), 4);
    }
}
