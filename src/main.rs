/*
 * The bulk of the application
 */

use aws_lambda_events::event::s3::{S3Event, S3EventRecord};
use lambda_runtime::{handler_fn, Context, Error};
use log::*;
use std::path::PathBuf;

mod config;
mod writer;

#[tokio::main]
async fn main() -> Result<(), Error> {
    #[cfg(debug_assertions)]
    pretty_env_logger::init();

    info!(
        "Initializing delta-s3-loader v{}",
        env!["CARGO_PKG_VERSION"]
    );

    let func = handler_fn(s3_event_handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

/**
 * The s3_event_handler will be invoked with an S3Event which will need to be iterated upon and
 * each S3EventRecord processed:
 *  <https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html>
 */
async fn s3_event_handler(event: S3Event, _ctx: Context) -> Result<String, Error> {
    // Unfortunately there's not a good way to avoid reload the configuration every time. At least
    // as far as I can tell right now
    let _conf = config::Config::from_file(&PathBuf::from("./config.yml"))?;

    for record in event.records {
        if let Some(ref name) = record.event_name {
            trace!("Processing an event named: {}", name);
            /*
             * The only events that delta-s3-loader is interested in are new PUTs which
             * indicate a new file must be processed.
             */
            if name == "ObjectCreated:Put" {
                trace!("Processing record: {:?}", record);
            }
        } else {
            warn!("Received a record without a name: {:?}", record);
        }
    }

    // Since this was triggered asynchronously, no need for a real response
    Ok("{}".to_string())
}

/**
 * The match_record function will look at a given S3EventRecord to see if the contents of that
 * record match a specific source configured for the Lambda
 *
 * Each of the records passed is assumed to be an `ObjectCreated:Put`
 */
fn match_record(record: &S3EventRecord, conf: &config::Config) -> Option<MatchedFile> {
    if let Some(bucket) = &record.s3.bucket.name {
        for source in &conf.sources {
            if bucket != &source.bucket {
                continue;
            }

            if let Some(key) = &record.s3.object.key {
                if !source.prefix.is_match(key) {
                    continue;
                }

                let expected_partitions = source.partitions.len();

                /* If there are no partitions expected, then we've already matched
                 */
                if expected_partitions == 0 {
                    return Some(MatchedFile::new(source, key, vec![]));
                }

                /*
                 * At this point the key and the prefix have matched, now to ensure
                 * that the data is partitioned according to the configuration
                 */
                let partitions = Partition::from_path_str(key);
                debug!(
                    "Partitions identified from record: {} -- {:?}",
                    key, partitions
                );

                // No match if the discovered partitions don't match the count
                if partitions.len() != expected_partitions {
                    continue;
                }

                for index in 0..expected_partitions {
                    // The partition at the index doesn't match what we're looking for
                    if partitions[index].name != source.partitions[index] {
                        return None;
                    }
                }

                return Some(MatchedFile::new(source, key, partitions));
            }
        }
    }
    None
}

/**
 * A MatchedFile will contain all the necessary information for reading the object and allowing a
 * Delta write to occur
 */
struct MatchedFile {
    /// The source bucket containing the file to read
    bucket: String,
    /// The source object to read, expected to be JSON
    object: String,
    /// Partitions to include in the Delta write
    partitions: Vec<Partition>,
    /// The output path for the Delta writer to use
    output_path: String,
}

impl MatchedFile {
    fn new(source: &config::Source, key: &str, partitions: Vec<Partition>) -> Self {
        MatchedFile {
            bucket: source.bucket.clone(),
            object: key.to_string(),
            partitions: partitions,
            output_path: source.tablepath.clone(),
        }
    }
}

/**
 * A Partition is a simple struct to carry values from paths like:
 *  just/a/path/year=2021/mydata.json
 */
#[derive(Clone, Debug)]
struct Partition {
    name: String,
    value: String,
}

impl Partition {
    /**
     * Convert the given path string (e.g. root/year=2021/month=04/afile.json)
     * into an ordered vector of the partitions contained within the path string
     */
    fn from_path_str(pathstr: &str) -> Vec<Partition> {
        pathstr
            .split("/")
            .filter_map(|part| {
                let sides: Vec<&str> = part.split("=").collect();
                if sides.len() == 2 {
                    Some(Partition {
                        name: sides[0].to_string(),
                        value: sides[1].to_string(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_path_str_no_partitions() {
        let result = Partition::from_path_str("just/a/path");
        assert_eq!(0, result.len());
    }

    #[test]
    fn from_path_str_single() {
        let partitions = Partition::from_path_str("some/year=2021/file.json");
        assert_eq!(1, partitions.len());
        assert_eq!(partitions[0].name, "year");
        assert_eq!(partitions[0].value, "2021");
    }

    /**
     * Make sure the sample event, which doesn't match a configured entry, doesn't return a source
     */
    #[test]
    fn test_match_record_fail() {
        let event: S3Event =
            serde_json::from_str(&sample_event()).expect("Failed to deserialize event");
        let conf = test_config();
        let matches = match_record(&event.records[0], &conf);
        assert!(matches.is_none());
    }

    #[test]
    fn test_match_record_ok() {
        let event: S3Event =
            serde_json::from_str(&sample_event()).expect("Failed to deserialize event");
        let conf = config::Config::new(vec![config::Source {
            bucket: "my-bucket".to_string(),
            prefix: regex::Regex::new("^somepath").expect("Failed to compile test regex"),
            partitions: vec!["date".to_string()],
            tablepath: "s3://test".to_string(),
        }]);
        let matches = match_record(&event.records[0], &conf);
        assert!(matches.is_some());
    }

    #[test]
    fn test_match_record_prefix_mismatch() {
        let event: S3Event =
            serde_json::from_str(&sample_event()).expect("Failed to deserialize event");
        let conf = config::Config::new(vec![config::Source {
            bucket: "my-bucket".to_string(),
            prefix: regex::Regex::new("^scoobydoo").expect("Failed to compile test regex"),
            partitions: vec!["date".to_string()],
            tablepath: "s3://test".to_string(),
        }]);
        let matches = match_record(&event.records[0], &conf);
        assert!(matches.is_none());
    }

    #[test]
    fn test_match_record_partition_mismatch() {
        let event: S3Event =
            serde_json::from_str(&sample_event()).expect("Failed to deserialize event");
        let conf = config::Config::new(vec![config::Source {
            bucket: "my-bucket".to_string(),
            prefix: regex::Regex::new("^somepath").expect("Failed to compile test regex"),
            partitions: vec!["year".to_string()],
            tablepath: "s3://test".to_string(),
        }]);
        let matches = match_record(&event.records[0], &conf);
        assert!(matches.is_none());
    }

    /**
     * Load the config.yml for tests
     */
    fn test_config() -> config::Config {
        config::Config::from_file(&PathBuf::from("./config.yml"))
            .expect("Failed to load configuration for tests")
    }
    fn sample_event() -> String {
        String::from(
            r#"
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-2",
      "eventTime": "2019-09-03T19:37:27.192Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AIDAINPONIXQXHT3IKHL2"
      },
      "requestParameters": {
        "sourceIPAddress": "205.255.255.255"
      },
      "responseElements": {
        "x-amz-request-id": "D82B88E5F771F645",
        "x-amz-id-2": "vlR7PnpV2Ce81l0PRw6jlUpck7Jo5ZsQjryTjKlc5aLWGVHPZLj5NeC6qMa0emYBDXOo6QBU0Wo="
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "828aa6fc-f7b5-4305-8584-487c791949c1",
        "bucket": {
          "name": "my-bucket",
          "ownerIdentity": {
            "principalId": "A3I5XTEXAMAI3E"
          },
          "arn": "arn:aws:s3:::lambda-artifacts-deafc19498e3f2df"
        },
        "object": {
          "key": "somepath/date=2021-04-16/afile.json",
          "size": 1305107,
          "eTag": "b21b84d653bb07b05b1e6b33684dc11b",
          "sequencer": "0C0F6F405D6ED209E1"
        }
      }
    }
  ]
}"#,
        )
    }

    #[tokio::test]
    async fn test_s3_event_handler() {
        let event: S3Event =
            serde_json::from_str(&sample_event()).expect("Failed to deserialize event");
        let result = s3_event_handler(event, Context::default())
            .await
            .expect("Failed to run event handler");
        assert_eq!("{}", result);
    }
}
