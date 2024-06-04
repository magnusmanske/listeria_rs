extern crate config;
extern crate serde_json;

use anyhow::Result;
use listeria::listeria_bot::ListeriaBot;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use wikimisc::toolforge_app::ToolforgeApp;

const MAX_INACTIVITY_BEFORE_SEPPUKU_SEC: u64 = 120;

/*
TEST DB CONNECT
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &

REFRESH FROM GIT
cd /data/project/listeria/listeria_rs ; ./build.sh

# RUN BOT ON TOOLFORGE
cd /data/project/listeria/listeria_rs ; ./restart.sh

*/

async fn run_singles(config_file: &str) -> Result<()> {
    let bot = ListeriaBot::new(config_file).await?;
    let max_threads = bot.config().max_threads();
    println!("Starting {max_threads} bots");
    let _ = bot.reset_running().await;
    let _ = bot.clear_deleted().await;
    let bot = Arc::new(bot);
    let bots_running = Arc::new(Mutex::new(0));
    let last_activity = ToolforgeApp::seppuku(MAX_INACTIVITY_BEFORE_SEPPUKU_SEC);
    loop {
        let page = match bot.prepare_next_single_page().await {
            Ok(page) => page,
            Err(e) => {
                eprintln!("{e}");
                continue;
            }
        };
        while *bots_running.lock().unwrap() >= max_threads {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        *bots_running.lock().unwrap() += 1;
        println!(
            "Starting new bot, {} running",
            *bots_running.lock().unwrap()
        );
        let bots_running = bots_running.clone();
        let bot = bot.clone();
        *last_activity.lock().unwrap() = Instant::now();
        tokio::spawn(async move {
            let pagestatus_id = page.id();
            let start_time = Instant::now();
            if let Err(e) = bot.run_single_bot(page).await {
                println!("{}", &e)
            }
            let end_time = Instant::now();
            let diff = (end_time - start_time).as_secs();
            let _ = bot.set_runtime(pagestatus_id, diff).await;
            bot.release_running(pagestatus_id).await;
            *bots_running.lock().unwrap() -= 1;
        });
    }
}

// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
// #[tokio::main(flavor = "multi_thread")]
fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_file = args
        .get(1)
        .map(|s| s.to_owned())
        .unwrap_or_else(|| "config.json".to_string());
    // run_singles(&config_file).await

    let file = std::fs::File::open(&config_file)?;
    let reader = std::io::BufReader::new(file);
    let j: serde_json::Value = serde_json::from_reader(reader)?;
    let threads = j["max_threads"].as_u64().unwrap_or(3) as usize;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(threads)
        .thread_name("listeria")
        // .thread_stack_size(threads * 1024 * 1024)
        // .thread_keep_alive(Duration::from_secs(600)) // 10 min
        .build()?
        .block_on(async move {
            let _ = run_singles(&config_file).await;
        });
    Ok(())
}
