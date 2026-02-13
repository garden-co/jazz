#![cfg(not(target_arch = "wasm32"))]

#[path = "support/scenarios.rs"]
mod scenarios;

use futures::executor::block_on;
use jazz_lsm::{LsmTree, StdFs};

use scenarios::TestResult;

fn open_with_defaults(fs: StdFs) -> TestResult<LsmTree<StdFs>> {
    Ok(LsmTree::open(
        fs,
        scenarios::test_options(),
        vec![scenarios::append_merge_op()],
    )?)
}

fn open_without_merge(fs: StdFs) -> TestResult<LsmTree<StdFs>> {
    Ok(LsmTree::open(fs, scenarios::test_options(), vec![])?)
}

#[test]
fn flush_wal_makes_acknowledged_write_survive_restart() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::flush_wal_survives_restart::<StdFs, _, _>(
        move || std::future::ready(open_with_defaults(fs.clone())),
    ))
}

#[test]
fn wal_replay_survives_second_restart_without_flush() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::wal_replay_survives_second_restart_without_flush::<
        StdFs,
        _,
        _,
    >(move || {
        std::future::ready(open_with_defaults(fs.clone()))
    }))
}

#[test]
fn unknown_merge_operator_is_rejected_on_open() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::unknown_merge_operator_rejected_on_open::<
        StdFs,
        _,
        _,
        _,
        _,
    >(
        {
            let fs = fs.clone();
            move || std::future::ready(open_with_defaults(fs.clone()))
        },
        move || std::future::ready(open_without_merge(fs.clone())),
    ))
}

#[test]
fn delete_dominates_merge_history() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::delete_dominates_merge_history::<StdFs, _, _>(
        move || std::future::ready(open_with_defaults(fs.clone())),
    ))
}

#[test]
fn range_scan_returns_sorted_live_values() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::range_scan_returns_sorted_live_values::<
        StdFs,
        _,
        _,
    >(move || {
        std::future::ready(open_with_defaults(fs.clone()))
    }))
}

#[test]
fn deepest_compaction_can_drop_safe_tombstones() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::deepest_compaction_drops_safe_tombstones::<
        StdFs,
        _,
        _,
    >(move || {
        std::future::ready(open_with_defaults(fs.clone()))
    }))
}

#[test]
fn flush_truncates_wal() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::flush_truncates_wal::<StdFs, _, _>(move || {
        std::future::ready(open_with_defaults(fs.clone()))
    }))
}

#[test]
fn crud_update_delete_range_correctness_large() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::crud_update_delete_range_correctness_large::<
        StdFs,
        _,
        _,
    >(move || {
        std::future::ready(open_with_defaults(fs.clone()))
    }))
}

#[test]
fn merge_and_multi_tier_correctness_large() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fs = StdFs::new(dir.path())?;

    block_on(scenarios::merge_and_multi_tier_correctness_large::<
        StdFs,
        _,
        _,
    >(move || {
        std::future::ready(open_with_defaults(fs.clone()))
    }))
}
