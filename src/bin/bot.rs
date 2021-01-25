extern crate config;
extern crate serde_json;

use listeria::listeria_bot::ListeriaBot;
use tokio::runtime;

/*
TEST DB CONNECT
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N

REFRESH FROM GIT
cd /data/project/listeria/listeria_rs ; git pull ; \rm ./target/release/bot ; jsub -mem 4g -cwd cargo build --release

# RUN BOT ON TOOLFORGE
cd ~/listeria_rs ; jsub -mem 6g -cwd -continuous ./target/release/bot

# TODO freq
*/

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let threaded_rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .thread_name("listeria")
        .thread_stack_size(3 * 1024 * 1024)
        .build()?;

    threaded_rt.block_on(async move {
        let mut bot = ListeriaBot::new("config.json").await.unwrap();
        loop {
            match bot.process_next_page().await {
                Ok(()) => {}
                Err(e) => {
                    println!("{}", &e);
                }
            }
        }
    });
    Ok(())
}
