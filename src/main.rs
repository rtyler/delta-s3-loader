/*
 * The bulk of the application
 */

use log::*;
use std::path::PathBuf;

mod config;
mod writer;
#[cfg(lambda)]
mod lambda;

fn preboot() {
    #[cfg(debug_assertions)]
    pretty_env_logger::init();

    info!(
        "Initializing delta-s3-loader v{}",
        env!["CARGO_PKG_VERSION"]
    );
}

#[cfg(not(lambda))]
fn main() {
    preboot();
}


#[cfg(lambda)]
#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    preboot();
    let func = lambda_runtime::handler_fn(crate::lambda::s3_event_handler);
    lambda_runtime::run(func).await?;
    Ok(())
}
