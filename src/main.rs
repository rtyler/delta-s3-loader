/*
 * The bulk of the application
 */
use clap::{App, Arg};
use log::*;

#[cfg(feature = "lambda")]
mod lambda;
mod writer;

fn preboot() {
    #[cfg(debug_assertions)]
    pretty_env_logger::init();

    info!(
        "Initializing delta-s3-loader v{}",
        env!["CARGO_PKG_VERSION"]
    );
    let _matches = App::new("delta-s3-loader")
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
            Arg::with_name("SQS queue ARN")
                .short("q")
                .long("queue")
                .value_name("QUEUE_ARN")
                .help("ARN of the SQS queue to consume, *required* in standalone mode")
                .takes_value(true),
        )
        .get_matches();
}

#[cfg(not(feature = "lambda"))]
fn main() {
    preboot();
}

#[cfg(feature = "lambda")]
#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    preboot();
    let func = lambda_runtime::handler_fn(crate::lambda::s3_event_handler);
    lambda_runtime::run(func).await?;
    Ok(())
}
