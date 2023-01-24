extern crate config;
extern crate serde_json;

use std::{sync::Arc, convert::TryInto};
use tokio::{sync::Mutex, runtime};
use listeria::listeria_bot::ListeriaBot;
use tokio::time::{sleep, Duration};

/*
TEST DB CONNECT
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N

REFRESH FROM GIT
cd /data/project/listeria/listeria_rs ; git pull ; \rm ./target/release/bot ; jsub -mem 4g -cwd cargo build --release

# RUN BOT ON TOOLFORGE
cd ~/listeria_rs ; jsub -mem 6g -cwd -continuous ./target/release/bot

# TODO freq
*/

const THREADS: usize = 4;

async fn run_singles() {
    let running_counter = Arc::new(Mutex::new(0 as i32));
    let bot = ListeriaBot::new("config.json").await.unwrap();
    let _ = bot.reset_running().await;
    let bot = Arc::new(bot);
    loop {
        while *running_counter.lock().await>=THREADS.try_into().expect("Can't convert THREADS to usize") {
            sleep(Duration::from_millis(5000)).await;
        }
        let page = match bot.prepare_next_single_page().await {
            Ok(page) => page,
            Err(_) => continue,
        };
        let bot = bot.clone();
        let running_counter = running_counter.clone();
        *running_counter.lock().await += 1 ;
        tokio::spawn(async move {
            // println!("Running: {} for {:?}",running_counter.lock().await,&page);
            if let Err(e) = bot.run_single_bot(page).await {
                println!("{}", &e)
            }
            *running_counter.lock().await -= 1 ;
        });
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
//#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /*
    let threaded_rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(THREADS)
        .thread_name("listeria")
        .thread_stack_size(THREADS * 1024 * 1024)
        .build()?;

    threaded_rt.block_on(async move { */
        run_singles().await;
    //});
    Ok(())
}
