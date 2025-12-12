use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use listeria::configuration::Configuration;
use listeria::entity_container_wrapper::EntityContainerWrapper;
use listeria::listeria_page::ListeriaPage;
use listeria::wiki_apis::WikiApis;
use std::fs::read_to_string;
use std::sync::Arc;
use wikimisc::wikibase::EntityTrait;

struct CliCommands {
    config: Arc<Configuration>,
}

impl CliCommands {
    async fn update_page(&self, page_title: &str, api_url: &str) -> Result<String> {
        let mut mw_api = wikimisc::mediawiki::api::Api::new(api_url).await?;
        mw_api.set_oauth2(self.config.oauth2_token());

        let mw_api = Arc::new(mw_api);
        let mut page = ListeriaPage::new(self.config.clone(), mw_api, page_title.into()).await?;
        page.run().await.map_err(|e| anyhow!("{e:?}"))?;

        if false {
            // FOR TESTING
            println!("{}", page.as_wikitext().await?[0]);
            Ok("OK".to_string())
        } else {
            Ok(
                match page
                    .update_source_page()
                    .await
                    .map_err(|e| anyhow!("{e:?}"))?
                {
                    true => format!("{page_title} edited"),
                    false => format!("{page_title} not edited"),
                },
            )
        }
    }

    async fn update_wikis(&self) -> Result<()> {
        let wiki_list = WikiApis::new(self.config.clone()).await?;
        wiki_list.update_wiki_list_in_database().await?;
        wiki_list.update_all_wikis().await?;
        Ok(())
    }

    async fn load_test_entities(&mut self) -> Result<()> {
        let mut items = vec![];
        for line in read_to_string("test_data/entities.tab").unwrap().lines() {
            items.push(line.to_string())
        }
        // These two can be missing for some reason?
        items.push("Q3".to_string());
        items.push("Q4".to_string());

        let config = Arc::get_mut(&mut self.config).unwrap();
        config.set_max_local_cached_entities(1000000); // A lot
        let ecw = EntityContainerWrapper::new().await?;
        let api = wikimisc::mediawiki::api::Api::new("https://www.wikidata.org/w/api.php").await?;
        ecw.load_entities(&api, &items).await?;

        let mut first = true;
        for item in items {
            let entity = match ecw.get_entity(&item).await {
                Some(e) => e,
                None => continue,
            };
            if first {
                println!("{{");
                first = false;
            } else {
                println!(",");
            }
            print!(
                "\"{item}\":{}",
                serde_json::to_string(&entity.to_json()).unwrap()
            );
        }
        println!("\n}}");
        Ok(())
    }

    async fn process_page(&self, server: &str, page: &str) -> Result<()> {
        let wiki_api = format!("https://{}/w/api.php", &server);
        let message = match self.update_page(page, &wiki_api).await {
            Ok(m) => format!("OK: {m}"),
            Err(e) => format!("ERROR: {e}"),
        };
        println!("{message}");
        Ok(())
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
    #[arg(short, long, default_value = "./config.json")]
    config: String,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Args::parse();

    let mut config = Configuration::new_from_file(&cli.config).await?;
    config.set_profiling(true); // Force profiling on for manual single job
    let mut main = CliCommands {
        config: Arc::new(config),
    };

    match cli.cmd {
        Commands::UpdateWikis => main.update_wikis().await,
        Commands::LoadTestEntities => main.load_test_entities().await,
        Commands::Page { server, page } => main.process_page(&server, &page).await,
    }
}

/*
ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &

To update the test_entities.json file:
cargo test -- --nocapture | grep entity_loaded | cut -f 1 | sort -u > test_data/entities.tab ; \
cargo run --bin main -- load-test-entities > test_data/test_entities.json

*/
