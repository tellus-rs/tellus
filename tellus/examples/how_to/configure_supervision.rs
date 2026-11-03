//! How-to example for configuring an actor from a file: an `ActorConfig` deserialized from TOML
//! with the `config` crate, and the error which contradicting backoff bounds produce.

use anyhow::Context;
use config::{Config, File, FileFormat};
use std::convert::Infallible;
use tellus::{
    Actor, ActorConfig, ActorContext, ActorSystem, Control, Incoming, Unstashed, Untimed,
};

const CONFIG_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/how_to/device.toml");

// ANCHOR: invalid
const INVALID: &str = r#"
[supervision_strategy.restart]
max_restarts = 3
backoff      = { min = "10s", max = "1s" }
"#;
// ANCHOR_END: invalid

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ANCHOR: load
    let config = Config::builder()
        .add_source(File::new(CONFIG_FILE, FileFormat::Toml))
        .build()
        .context("reading the configuration")?
        .try_deserialize::<ActorConfig>()
        .context("deserializing the actor configuration")?;
    let system = ActorSystem::with_config(Device, config);
    // ANCHOR_END: load
    println!("Loaded: {config:?}");

    // ANCHOR: error
    let error = Config::builder()
        .add_source(File::from_str(INVALID, FileFormat::Toml))
        .build()
        .context("reading the configuration")?
        .try_deserialize::<ActorConfig>()
        .expect_err("contradicting backoff bounds are rejected");
    println!("Rejected: {error}");
    // ANCHOR_END: error

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}

struct Device;

impl Actor for Device {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(()))
    }
}
