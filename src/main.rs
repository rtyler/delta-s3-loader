/*
 * The bulk of the application
 */
use aws_lambda_events::event::s3::S3Event;
use clap::{App, Arg, ArgMatches};
use log::*;
use tokio::io::AsyncReadExt;

#[cfg(feature = "lambda")]
mod lambda;
mod writer;

/**
 * Preboot is responsible for the processing of arguments and other initialization
 * tasks common to both the standalone and lambda modes of operation
 */
fn preboot() -> ArgMatches<'static> {
    dotenv::dotenv().ok();

    #[cfg(debug_assertions)]
    pretty_env_logger::init();

    info!(
        "Initializing delta-s3-loader v{}",
        env!["CARGO_PKG_VERSION"]
    );

    App::new("delta-s3-loader")
        .version(env!["CARGO_PKG_VERSION"])
        .author("rtyler@brokenco.de")
        .about("Watch S3 buckets for new data to load into a Delta Table")
        .arg(
            Arg::with_name("table")
                .required(true)
                .short("t")
                .long("table")
                .env("TABLE_PATH")
                .value_name("TABLE_PATH")
                .help("Sets the destination Delta Table for the ingested data")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("partitions")
                .short("p")
                .long("partitions")
                .env("CSV_PARTITIONS")
                .value_name("CSV_PARTITIONS")
                .help("Ordered list of partition from the source data, e.g. year,month")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("queue")
                .short("q")
                .long("queue")
                .env("QUEUE_URL")
                .value_name("URL")
                .help("URL of the SQS queue to consume, *required* in standalone mode")
                .takes_value(true),
        )
        .get_matches()
}

#[cfg(not(feature = "lambda"))]
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    use rusoto_core::*;
    use rusoto_sqs::*;

    let args = preboot();

    if let Some(queue) = args.value_of("queue") {
        info!("Running in standalone mode with the queue: {}", queue);
        let client = SqsClient::new(Region::default());

        loop {
            let rcv = ReceiveMessageRequest {
                queue_url: queue.to_string(),
                // Always consuming the max number of available messages to reduce round trips
                max_number_of_messages: Some(10),
                // This wait time is being used as the effective loop interval for the main runloop
                wait_time_seconds: Some(10),
                ..Default::default()
            };

            let res = client.receive_message(rcv).await?;
            trace!("Message(s) received: {:?}", res);

            if let Some(messages) = &res.messages {
                for message in messages {
                    let mut should_delete = true;

                    if let Some(body) = &message.body {
                        match serde_json::from_str::<S3Event>(&body) {
                            Ok(event) => {
                                debug!("Parsed an event to do something with: {:?}", event);
                                should_delete = match process_event(&event).await {
                                    Ok(_) => true,
                                    Err(e) => {
                                        error!("Failure when processing event: {}", e);
                                        false
                                    }
                                };
                            }
                            Err(err) => {
                                error!("Failed to deserialize what should have been an S3Event: {:?} - {}", &message.message_id, err);
                            }
                        }
                    } else {
                        warn!("Message had no body: {:?}", &message);
                    }

                    if should_delete {
                        match &message.receipt_handle {
                            Some(receipt) => {
                                let delete = DeleteMessageRequest {
                                    queue_url: queue.to_string(),
                                    receipt_handle: receipt.to_string(),
                                };
                                // TODO: More gracefully handle this error
                                client.delete_message(delete).await?;
                            }
                            None => {
                                warn!("Somehow a message without a receipt handle was received!");
                            }
                        }
                    }
                }
            }
        }
    } else {
        panic!("When running in standalone mode the `queue` argument (or QUEUE_URL env variable) must be present");
    }
}

#[cfg(feature = "lambda")]
#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    preboot();
    let func = lambda_runtime::handler_fn(crate::lambda::s3_event_handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

/**
 * URL decode the given string
 */
fn urldecode(buf: &str) -> String {
    use form_urlencoded::parse;
    parse(buf.as_bytes())
        .map(|(key, val)| [key, val].concat())
        .collect::<String>()
}

/**
 * Process an event
 */
async fn process_event(event: &S3Event) -> Result<(), anyhow::Error> {
    use rusoto_core::*;
    use rusoto_s3::*;

    let client = S3Client::new(Region::default());

    for record in &event.records {
        // NOTE: This is gross, there's got to be a better way
        let bucket = record.s3.bucket.name.as_ref().unwrap().to_string();
        // Key names with partitions will be url encoded which means they need to
        // be processed first
        let key = urldecode(record.s3.object.key.as_ref().unwrap());

        let get = GetObjectRequest {
            key,
            bucket,
            ..Default::default()
        };

        trace!("request: {:?}", get);
        let result = client.get_object(get).await?;
        trace!("result: {:?}", result);
        if let Some(stream) = result.body {
            let mut buf = Vec::new();
            stream.into_async_read().read_to_end(&mut buf).await;
        } else {
            warn!("Object had no body, somehow");
        }
    }
    Err(anyhow::Error::msg("Fail"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urldecode_with_encodes() {
        let buf = "date%3D2021-03-12/auditlog-1235.json";
        assert_eq!("date=2021-03-12/auditlog-1235.json", urldecode(&buf));
    }

    #[test]
    fn test_urldecode_normal() {
        let buf = "raw/auditlog-1235.json";
        assert_eq!("raw/auditlog-1235.json", urldecode(&buf));
    }
}
