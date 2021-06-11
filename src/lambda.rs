use aws_lambda_events::event::s3::S3Event;
use lambda_runtime::{Context, Error};
use log::*;

/**
 * The s3_event_handler will be invoked with an S3Event which will need to be iterated upon and
 * each S3EventRecord processed:
 *  <https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html>
 */
pub async fn s3_event_handler(event: S3Event, _ctx: Context) -> Result<String, Error> {
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
