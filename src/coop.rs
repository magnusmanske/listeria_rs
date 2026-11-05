//! Cooperative yielding for CPU-heavy loops on the async runtime.
//!
//! Much of the per-list work is plain CPU work with no real await point: a
//! `join_all` over every result row, or a loop whose awaited calls return at
//! once. That keeps the runtime worker busy for as long as the loop runs — no
//! other task is polled, so timers do not fire and the once-a-minute telemetry
//! push can be minutes late, which the dashboard shows as "stale". It also
//! keeps the per-page timeout from expiring.
//!
//! A small CPU quota makes this worse: tokio sizes its worker pool from the
//! available parallelism, which a cgroup CPU limit can reduce to one thread.
//!
//! [`yield_every`] gives such loops an await point every [`ITEMS_PER_YIELD`]
//! items; the constant also sizes chunks for `join_all`-style loops.

/// Items a CPU-heavy loop may process before yielding to the runtime.
///
/// Small enough that one chunk stays well below a second on a throttled CPU,
/// large enough that the yield costs nothing measurable.
pub(crate) const ITEMS_PER_YIELD: usize = 128;

/// Yields to the runtime every [`ITEMS_PER_YIELD`] items of a loop, `index`
/// being the loop counter (from `.enumerate()` or `0..len`).
pub(crate) async fn yield_every(index: usize) {
    if index.is_multiple_of(ITEMS_PER_YIELD) {
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(flavor = "current_thread")]
    async fn test_yield_every_lets_another_task_run() {
        // A CPU-only loop would otherwise run to completion before the
        // spawned task is polled at all.
        let ticks = Arc::new(AtomicUsize::new(0));
        let counter = ticks.clone();
        tokio::spawn(async move {
            counter.fetch_add(1, Ordering::SeqCst);
        });

        for i in 0..ITEMS_PER_YIELD * 4 {
            yield_every(i).await;
        }

        assert!(ticks.load(Ordering::SeqCst) > 0);
    }
}
