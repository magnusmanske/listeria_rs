use anyhow::Result;
use sysinfo::System;

use crate::{configuration::Configuration, page_to_process::PageToProcess};

#[allow(async_fn_in_trait)]
pub trait ListeriaBot {
    async fn new(config_file: &str) -> Result<Self>
    where
        Self: Sized;
    fn config(&self) -> &Configuration;
    async fn reset_running(&self) -> Result<()>;
    async fn clear_deleted(&self) -> Result<()>;
    async fn set_runtime(&self, pagestatus_id: u64, seconds: u64) -> Result<()>;
    async fn run_single_bot(&self, page: PageToProcess) -> Result<()>;

    /// Removed a pagestatus ID from the running list
    async fn release_running(&self, pagestatus_id: u64);

    /// Returns how many pages are running
    async fn get_running_count(&self) -> usize;

    /// Returns a page to be processed.
    async fn prepare_next_single_page(&self) -> Result<PageToProcess>;

    fn print_sysinfo() {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return;
        }
        let sys = System::new_all();
        // println!("Uptime: {:?}", System::uptime());
        println!(
            "Memory: total {}, free {}, used {} MB",
            sys.total_memory() / 1024,
            sys.free_memory() / 1024,
            sys.used_memory() / 1024
        );
        println!(
            "Swap: total: {}, free {}, used:{} MB",
            sys.total_swap() / 1024,
            sys.free_swap() / 1024,
            sys.used_swap() / 1024
        );
        println!(
            "Processes: {}, CPUs: {}",
            sys.processes().len(),
            sys.cpus().len()
        );
        println!(
            "CPU usage: {}%, Load average: {:?}",
            sys.global_cpu_usage(),
            System::load_average()
        );
    }
}
