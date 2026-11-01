//! A terminal observer for the cluster demo: it follows two event streams per node (the inspect
//! crate's `/inspect/events` and the demo's `/events`) plus the verifier's, and polls `/cluster`,
//! `/probe` and `/status` slowly. It shows the membership matrix (what each node says about every
//! address), the selected node's detail, the verifier's counters and a merged timeline, ordered by
//! the servers' own timestamps. It changes nothing; the chaos agent keeps running.

mod config;
mod model;
mod sources;
mod sse;
mod ui;

use crate::{config::Config, model::App, sources::Input};
use crossterm::event::{Event, EventStream};
use futures_util::StreamExt;
use ratatui::DefaultTerminal;
use std::time::Duration;
use tellus_cluster_inspect::events::now_millis;
use tokio::{sync::mpsc, time::interval};

const TICK: Duration = Duration::from_millis(250);
const INPUT_BUFFER: usize = 1_024;
const DRAIN_PER_FRAME: usize = 256;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let (tx, rx) = mpsc::channel(INPUT_BUFFER);
    sources::spawn(&config, tx);

    let mut terminal = ratatui::init();
    let result = run(&mut terminal, rx, App::new(&config)).await;
    ratatui::restore();

    result
}

async fn run(
    terminal: &mut DefaultTerminal,
    mut rx: mpsc::Receiver<Input>,
    mut app: App,
) -> anyhow::Result<()> {
    let mut keys = EventStream::new();
    let mut ticks = interval(TICK);

    loop {
        terminal.draw(|frame| ui::draw(frame, &app))?;

        tokio::select! {
            event = keys.next() => match event {
                Some(Ok(Event::Key(key))) => app.apply(Input::Key(key), now_millis()),
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.into()),
                None => return Ok(()),
            },

            input = rx.recv() => {
                let Some(input) = input else {
                    return Ok(());
                };
                app.apply(input, now_millis());
                for _ in 0..DRAIN_PER_FRAME {
                    match rx.try_recv() {
                        Ok(input) => app.apply(input, now_millis()),
                        Err(_) => break,
                    }
                }
            }

            _ = ticks.tick() => app.apply(Input::Tick, now_millis()),
        }

        if app.quit {
            return Ok(());
        }
    }
}
