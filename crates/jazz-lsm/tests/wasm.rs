#![cfg(target_arch = "wasm32")]

#[path = "support/scenarios.rs"]
mod scenarios;

use jazz_lsm::{LsmTree, OpfsFs};
use wasm_bindgen_test::*;

use scenarios::TestResult;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

async fn open_with_defaults(namespace: &str) -> TestResult<LsmTree<OpfsFs>> {
    let fs = OpfsFs::open(namespace).await?;
    Ok(LsmTree::open(
        fs,
        scenarios::test_options(),
        vec![scenarios::append_merge_op()],
    )?)
}

async fn open_without_merge(namespace: &str) -> TestResult<LsmTree<OpfsFs>> {
    let fs = OpfsFs::open(namespace).await?;
    Ok(LsmTree::open(fs, scenarios::test_options(), vec![])?)
}

fn unique_namespace(name: &str) -> String {
    let ts = js_sys::Date::now() as u64;
    let rand = (js_sys::Math::random() * 1_000_000.0) as u64;
    format!("jazz-lsm-{name}-{ts}-{rand}")
}

#[wasm_bindgen_test]
async fn flush_wal_makes_acknowledged_write_survive_restart() {
    let ns = unique_namespace("flush-wal");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");
    let ns_for_open = ns.clone();

    scenarios::flush_wal_survives_restart::<OpfsFs, _, _>(move || {
        let ns = ns_for_open.clone();
        async move { open_with_defaults(&ns).await }
    })
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}

#[wasm_bindgen_test]
async fn unknown_merge_operator_is_rejected_on_open() {
    let ns = unique_namespace("unknown-merge");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");

    scenarios::unknown_merge_operator_rejected_on_open::<OpfsFs, _, _, _, _>(
        {
            let ns = ns.clone();
            move || {
                let ns = ns.clone();
                async move { open_with_defaults(&ns).await }
            }
        },
        {
            let ns = ns.clone();
            move || {
                let ns = ns.clone();
                async move { open_without_merge(&ns).await }
            }
        },
    )
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}

#[wasm_bindgen_test]
async fn delete_dominates_merge_history() {
    let ns = unique_namespace("delete-dominates");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");
    let ns_for_open = ns.clone();

    scenarios::delete_dominates_merge_history::<OpfsFs, _, _>(move || {
        let ns = ns_for_open.clone();
        async move { open_with_defaults(&ns).await }
    })
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}

#[wasm_bindgen_test]
async fn range_scan_returns_sorted_live_values() {
    let ns = unique_namespace("range-scan");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");
    let ns_for_open = ns.clone();

    scenarios::range_scan_returns_sorted_live_values::<OpfsFs, _, _>(move || {
        let ns = ns_for_open.clone();
        async move { open_with_defaults(&ns).await }
    })
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}

#[wasm_bindgen_test]
async fn deepest_compaction_can_drop_safe_tombstones() {
    let ns = unique_namespace("drop-tombstone");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");
    let ns_for_open = ns.clone();

    scenarios::deepest_compaction_drops_safe_tombstones::<OpfsFs, _, _>(move || {
        let ns = ns_for_open.clone();
        async move { open_with_defaults(&ns).await }
    })
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}

#[wasm_bindgen_test]
async fn flush_truncates_wal() {
    let ns = unique_namespace("flush-truncates-wal");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");
    let ns_for_open = ns.clone();

    scenarios::flush_truncates_wal::<OpfsFs, _, _>(move || {
        let ns = ns_for_open.clone();
        async move { open_with_defaults(&ns).await }
    })
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}

#[wasm_bindgen_test]
async fn crud_update_delete_range_correctness_large() {
    let ns = unique_namespace("crud-large");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");
    let ns_for_open = ns.clone();

    scenarios::crud_update_delete_range_correctness_large::<OpfsFs, _, _>(move || {
        let ns = ns_for_open.clone();
        async move { open_with_defaults(&ns).await }
    })
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}

#[wasm_bindgen_test]
async fn merge_and_multi_tier_correctness_large() {
    let ns = unique_namespace("merge-tier-large");
    OpfsFs::destroy(&ns).await.expect("cleanup before test");
    let ns_for_open = ns.clone();

    scenarios::merge_and_multi_tier_correctness_large::<OpfsFs, _, _>(move || {
        let ns = ns_for_open.clone();
        async move { open_with_defaults(&ns).await }
    })
    .await
    .expect("scenario should pass");

    OpfsFs::destroy(&ns).await.expect("cleanup after test");
}
