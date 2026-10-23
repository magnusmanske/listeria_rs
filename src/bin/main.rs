//! CLI entry point for the Listeria bot.

use anyhow::Result;
use clap::{Parser, Subcommand};
use listeria::{configuration::Configuration, main_commands::MainCommands, telemetry};
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
    /// The tool's website, for the multi-wiki bot (on `PORT`, default 8000)
    Webserver,
}

impl Commands {
    /// The dashboard component of a long-running mode; `None` for one-off commands.
    const fn service_component(&self) -> Option<&'static str> {
        match self {
            Self::Wikidata => Some("bot"),
            Self::SingleWiki { .. } => Some("single-wiki"),
            Self::Webserver => Some("web"),
            Self::UpdateWikis | Self::LoadTestEntities | Self::Page { .. } => None,
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // `tracing` to stderr, with `log` bridged in; filter from `RUST_LOG`
    // (e.g. `RUST_LOG=listeria=debug`), else `info`.
    wikimisc::logging::init();
    let cli = Args::parse();

    let config_file = cli.config;
    let config = Configuration::new_from_file(&config_file).await?;

    let config = match cli.cmd.service_component() {
        Some(component) => {
            // The website shuts down gracefully, and sends the final push itself
            let handle_signals = !matches!(cli.cmd, Commands::Webserver);
            telemetry::install(component, &config, handle_signals);
            config
        }
        // Enable profiling for single-use commands (page, update-wikis, load-test-entities).
        None => config.with_profiling(true),
    };

    let mut main = MainCommands {
        config: Arc::new(config),
        config_file,
    };

    let result = match cli.cmd {
        Commands::UpdateWikis => main.update_wikis().await,
        Commands::LoadTestEntities => main.load_test_entities().await,
        Commands::Page { server, page } => main.process_page(&server, &page).await,
        Commands::Wikidata => main.run_wikidata_bot().await,
        Commands::SingleWiki { once } => main.run_single_wiki_bot(once).await,
        Commands::Webserver => main.run_webserver().await,
    };
    telemetry::shutdown(&result).await;
    result
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
