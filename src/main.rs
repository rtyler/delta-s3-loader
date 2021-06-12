/*
 * The bulk of the application
 */
use aws_lambda_events::event::s3::S3Event;
use clap::{App, Arg, ArgMatches};
use log::*;

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
                    if let Some(body) = &message.body {
                        match serde_json::from_str::<S3Event>(&body) {
                            Ok(event) => {
                                debug!("Parsed an event to do something with: {:?}", event);
                            }
                            Err(err) => {
                                error!("Failed to deserialize what should have been an S3Event: {:?} - {}", &message.message_id, err);
                            }
                        }
                    } else {
                        warn!("Message had no body: {:?}", &message);
                    }

                    if let Some(receipt) = &message.receipt_handle {
                        let delete = DeleteMessageRequest {
                            queue_url: queue.to_string(),
                            receipt_handle: receipt.to_string(),
                        };
                        // TODO: More gracefully handle this error
                        client.delete_message(delete).await?;
                    } else {
                        warn!("Somehow a message without a receipt handle was received!");
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
