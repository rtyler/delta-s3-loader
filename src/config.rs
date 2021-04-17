/**
 * The config module is largely responsible for just reading a configuration yaml file in.
 */
use regex::Regex;
use std::path::PathBuf;
use serde::Deserialize;

/**
 * Root configuration
 */
#[derive(Clone, Debug, Deserialize)]
struct Config {
    sources: Vec<Source>,
}

impl Config {
    /**
     * from_file will attempt to load a configuration from the given path
     *
     * The path should be to a properly formatted YAML file:
     */
    fn from_file(path: &PathBuf) -> Result<Config, std::io::Error> {
        use std::io::{Error, ErrorKind};
        use std::fs::File;

        let reader = File::open(path)?;

        serde_yaml::from_reader(reader)
            .map_err(|e| Error::new(ErrorKind::Other, e))
    }
}


/**
 * A source maps an existing S3 structure into a named Delta table
 */
#[derive(Clone, Debug, Deserialize)]
struct Source {
    bucket: String,
    #[serde(with = "serde_regex")]
    prefix: Regex,
    partitions: Vec<String>,
    database: String,
    table: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_file() -> Result<(), std::io::Error> {
        let config = Config::from_file(&PathBuf::from("./config.yml"))?;
        assert_eq!(config.sources.len(), 1);
        Ok(())
    }
}
