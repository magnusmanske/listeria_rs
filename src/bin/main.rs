use anyhow::Result;
use clap::{Parser, Subcommand};
use listeria::{configuration::Configuration, main_commands::MainCommands};
use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "./config.json")]
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
    let cli = Args::parse();

    let config_file = cli.config;
    let mut config = Configuration::new_from_file(&config_file).await?;

    // Set the profiling flag for single use commands only
    match cli.cmd {
        Commands::Wikidata | Commands::SingleWiki { once: _ } => {}
        _ => config.set_profiling(true),
    }

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
