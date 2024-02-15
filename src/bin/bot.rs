extern crate config;
extern crate serde_json;

use anyhow::Result;
use std::sync::Arc;
use listeria::listeria_bot::ListeriaBot;
use tokio::time::{sleep, Duration};

/*
TEST DB CONNECT
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &

REFRESH FROM GIT
cd /data/project/listeria/listeria_rs ; git pull ; \rm ./target/release/bot ; jsub -mem 4g -sync y -cwd cargo build --release

# RUN BOT ON TOOLFORGE
toolforge-jobs delete rustbot && toolforge-jobs delete rustbot2 && \
rm ~/rustbot* && \
toolforge-jobs run --image tf-php74 --mem 2500Mi --continuous --command '/data/project/listeria/listeria_rs/run.sh 4' rustbot && \
toolforge-jobs run --image tf-php74 --mem 2500Mi --continuous --command '/data/project/listeria/listeria_rs/run.sh 4' rustbot2

*/

async fn run_singles() -> Result<()> {
    let bot = ListeriaBot::new("config.json").await?;
    let max_threads = bot.config().max_threads();
    println!("Starting {max_threads} bots");
    let _ = bot.reset_running().await;
    let bot = Arc::new(bot);
    loop {
        while bot.get_running_count().await>=max_threads {
            sleep(Duration::from_millis(100)).await;
        }
        let page = match bot.prepare_next_single_page().await {
            Ok(page) => page,
            Err(e) => {
                eprintln!("{e}");
                continue
            }
        };
        // println!("{page:?}");
        let bot = bot.clone();
        tokio::spawn(async move {
            let pagestatus_id = page.id;
            if let Err(e) = bot.run_single_bot(page).await {
                println!("{}", &e)
            }
            bot.release_running(pagestatus_id).await;
        });
    }
}

//#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
#[tokio::main]
async fn main() -> Result<()> {
    // let threaded_rt = runtime::Builder::new_multi_thread()
    //     .enable_all()
    //     .worker_threads(threads)
    //     .thread_name("listeria")
    //     .thread_stack_size(threads * 1024 * 1024)
    //     .thread_keep_alive(Duration::from_secs(300)) // 5 min
    //     .build()?;

    // threaded_rt.block_on(async move {
    //     run_singles(threads).await;
    // });
    run_singles().await
}
