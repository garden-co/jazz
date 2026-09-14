//! The historical walltime backfill does not expose newer engine experiment APIs.
pub fn run(_: &std::path::Path) {
    panic!(
        "history-cost profiling requires post-alpha.54 engine APIs; unavailable in release backfill"
    );
}
