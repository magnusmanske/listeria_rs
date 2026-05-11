//! CLI entry point for the Listeria bot.

use anyhow::Result;
use clap::{Parser, Subcommand};
use listeria::{configuration::Configuration, main_commands::MainCommands};
use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "/etc/app/config.json")]
    config: String,
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    UpdateWikis,
    LoadTestEntities,
    Page {
        #[arg(short, long)]
        server: String,
        #[arg(short, long)]
        page: String,
    },
    Wikidata,
    SingleWiki {
        #[arg(short, long, default_value = "false")]
        once: bool,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Args::parse();

    let config_file = cli.config;
    let config = Configuration::new_from_file(&config_file).await?;

    // Enable profiling for single-use commands (page, update-wikis, load-test-entities).
    let config = match cli.cmd {
        Commands::Wikidata | Commands::SingleWiki { once: _ } => config,
        _ => config.with_profiling(true),
    };

    let mut main = MainCommands {
        config: Arc::new(config),
        config_file,
    };

    match cli.cmd {
        Commands::UpdateWikis => main.update_wikis().await,
        Commands::LoadTestEntities => main.load_test_entities().await,
        Commands::Page { server, page } => main.process_page(&server, &page).await,
        Commands::Wikidata => main.run_wikidata_bot().await,
        Commands::SingleWiki { once } => main.run_single_wiki_bot(once).await,
    }
}

/// Installs a `tracing` subscriber and a `log` → `tracing` bridge.
///
/// The bridge forwards every existing `log::info!` / `log::warn!` / ...
/// call into the tracing pipeline, so library code that still uses the
/// `log` macros keeps emitting visible output without modification. The
/// filter defaults to `info` and can be overridden via the standard
/// `RUST_LOG` env var (e.g. `RUST_LOG=listeria=debug`).
fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};

    // Forward `log::*!` events into the tracing event pipeline. Failing
    // here just means a previous logger was already installed in tests
    // or by a host process — that's fine, drop on the floor.
    let _ = tracing_log::LogTracer::init();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // `try_init` because some test harness paths may install a subscriber
    // before this code runs; silently treat a re-install as a no-op.
    let _ = fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

/*
TEST DB CONNECT
ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &

To update the test_entities.json file:
cargo test -- --nocapture | grep entity_loaded | cut -f 1 | sort -u > test_data/entities.tab ; \
cargo run --bin main -- load-test-entities > test_data/test_entities.json

REFRESH FROM GIT
cd /data/project/listeria/listeria_rs ; ./build.sh

# RUN BOT ON TOOLFORGE
cd /data/project/listeria/listeria_rs ; ./restart.sh
*/
