use anyhow::{Context, bail};
use std::env;

const NODES: &str = "TELLUS_NODES";
const VERIFIER: &str = "TELLUS_VERIFIER";
const DEFAULT_NODES: &str = "http://localhost:8091,http://localhost:8092,http://localhost:8093,\
                             http://localhost:8094,http://localhost:8095";
const DEFAULT_VERIFIER: &str = "http://localhost:8081";

pub struct Config {
    pub nodes: Vec<String>,
    pub verifier: String,
}

impl Config {
    /// The nodes' and the verifier's base URLs from `TELLUS_NODES` and `TELLUS_VERIFIER`, by
    /// default the host ports both demo stacks map.
    ///
    /// # Errors
    /// Fails if either variable is set but is not valid Unicode, or if `TELLUS_NODES` names no
    /// node.
    pub fn from_env() -> anyhow::Result<Self> {
        let nodes = var(NODES, DEFAULT_NODES)?
            .split(',')
            .map(|url| url.trim().trim_end_matches('/').to_string())
            .filter(|url| !url.is_empty())
            .collect::<Vec<_>>();
        if nodes.is_empty() {
            bail!("{NODES} names no node");
        }
        let verifier = var(VERIFIER, DEFAULT_VERIFIER)?
            .trim()
            .trim_end_matches('/')
            .to_string();

        Ok(Self { nodes, verifier })
    }
}

fn var(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.to_string()),
        Err(error) => Err(error).with_context(|| format!("{name} is not valid unicode")),
    }
}
