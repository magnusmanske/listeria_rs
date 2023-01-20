extern crate config;
extern crate serde_json;

//use std::sync::Arc;
//use tokio::sync::Mutex;
use listeria::listeria_bot::ListeriaBot;
//use tokio::time::{sleep, Duration};
//use tokio::runtime;

/*
TEST DB CONNECT
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N

REFRESH FROM GIT
cd /data/project/listeria/listeria_rs ; git pull ; \rm ./target/release/bot ; jsub -mem 4g -cwd cargo build --release

# RUN BOT ON TOOLFORGE
cd ~/listeria_rs ; jsub -mem 6g -cwd -continuous ./target/release/bot

# TODO freq
*/

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /*
    let threaded_rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .thread_name("listeria")
        .thread_stack_size(3 * 1024 * 1024)
        .build()?;

    threaded_rt.block_on(async move { */

    let bot = ListeriaBot::new("config.json").await.unwrap();
    loop {
        if let Err(e) = bot.process_next_page().await { println!("{}", &e);};
    }

    /*
    let running_counter = Arc::new(Mutex::new(0 as i32));
    let bot = ListeriaBot::new("config.json").await.unwrap();
    let bot = Arc::new(bot);
    loop {
        while *running_counter.lock().await>=8 {
            sleep(Duration::from_millis(5000)).await;
        }
        let bot = bot.clone();
        let running_counter = running_counter.clone();
        tokio::spawn(async move {
            *running_counter.lock().await += 1 ;
            println!("Running: {}",running_counter.lock().await);
            match bot.process_next_page().await {
                Ok(()) => {
                    *running_counter.lock().await -= 1 ;
                }
                Err(e) => {
                    *running_counter.lock().await -= 1 ;
                    println!("{}", &e);
                }
            }
        });
    }

    */
    //});
    //Ok(())
}
