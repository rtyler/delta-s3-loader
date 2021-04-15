/**
 * The writer module contains the important code for actually writing to a Delta Lake table
 *
 */

use arrow::record_batch::RecordBatch;
use serde_json::Value;

enum WriterError {
    Generic,
}

fn json_to_batch(json: Vec<Value>) { //-> Result<RecordBatch, WriterError>  {
    use arrow::json::reader::*;

    // infer_json_schema_from_iterator is weird in that it expects each value to be wrapped in a
    // Result
    let schema = infer_json_schema_from_iterator(
        json.into_iter().map(|v| Ok(v)));

    println!("schema: {:#?}", schema);

    //Err(WriterError::Generic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[test]
    fn demo() {
        let delta = deltalake::get_backend_for_uri("./data");
    }

    #[test]
    fn json_to_arrow_success() {
        let value: Vec<serde_json::Value> = serde_json::from_str(r#"
        [
            {
                "action" : "commit",
                "actor" : "rtyler"
            },
            {
                "action" : "update",
                "actor" : "rtyler"
            }
        ]
        "#).expect("Failed to create JSON");


        let result = json_to_batch(value);
        assert!(false);
        //assert!(result.is_ok());
    }
}
