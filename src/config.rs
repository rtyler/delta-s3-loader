/**
 * The config module is largely responsible for just reading a configuration yaml file in.
 */
use regex::Regex;
use serde::Deserialize;
use std::path::PathBuf;

/**
 * Root configuration
 */
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub sources: Vec<Source>,
}

impl Config {
    pub fn new(sources: Vec<Source>) -> Config {
        Config { sources }
    }
    /**
     * from_file will attempt to load a configuration from the given path
     *
     * The path should be to a properly formatted YAML file:
     */
    pub fn from_file(path: &PathBuf) -> Result<Config, std::io::Error> {
        use std::fs::File;
        use std::io::{Error, ErrorKind};

        let reader = File::open(path)?;

        serde_yaml::from_reader(reader).map_err(|e| Error::new(ErrorKind::Other, e))
    }
}

/**
 * A source maps an existing S3 structure into a named Delta table
 */
#[derive(Clone, Debug, Deserialize)]
pub struct Source {
    pub bucket: String,
    #[serde(with = "serde_regex")]
    pub prefix: Regex,
    pub partitions: Vec<String>,
    pub tablepath: String,
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
