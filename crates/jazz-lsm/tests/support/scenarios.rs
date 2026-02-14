use core::future::Future;
use std::collections::BTreeMap;

use jazz_lsm::{LsmOptions, LsmTree, MergeOperator, SyncFs, WriteDurability};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub fn append_merge_op() -> MergeOperator {
    MergeOperator {
        id: 1,
        name: "append".to_string(),
        apply: Box::new(|base, operand| {
            let mut out = base.map(|b| b.to_vec()).unwrap_or_default();
            out.extend_from_slice(operand);
            out
        }),
    }
}

pub fn test_options() -> LsmOptions {
    LsmOptions {
        max_memtable_bytes: 256,
        max_wal_bytes: 1024,
        level0_file_limit: 2,
        level_fanout: 2,
        max_levels: 2,
        write_durability: WriteDurability::Buffered,
        ..Default::default()
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn val(prefix: &str, i: usize) -> Vec<u8> {
    format!("{prefix}:{i:06}").into_bytes()
}

fn append_merge(base: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    let mut out = base.map(|b| b.to_vec()).unwrap_or_default();
    out.extend_from_slice(operand);
    out
}

pub async fn flush_wal_survives_restart<FS, Open, OpenFut>(mut open: Open) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    {
        let mut db = open().await?;
        db.put(b"k1", b"v1")?;
        db.flush_wal()?;
    }

    {
        let db = open().await?;
        assert_eq!(db.get(b"k1")?, Some(b"v1".to_vec()));
    }

    Ok(())
}

pub async fn wal_replay_survives_second_restart_without_flush<FS, Open, OpenFut>(
    mut open: Open,
) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    // First run: write + durable WAL sync, but do not checkpoint to SST.
    {
        let mut db = open().await?;
        db.put(b"k1", b"v1")?;
        db.flush_wal()?;
    }

    // First restart: record is recovered from WAL into memory.
    // Simulate a second hard crash before any flush/checkpoint.
    {
        let db = open().await?;
        assert_eq!(db.get(b"k1")?, Some(b"v1".to_vec()));
        assert!(
            db.debug_state()?.wal_bytes > 0,
            "WAL should still exist before checkpointing"
        );
    }

    // Second restart: record must still be recovered from WAL.
    {
        let db = open().await?;
        assert_eq!(db.get(b"k1")?, Some(b"v1".to_vec()));
    }

    Ok(())
}

pub async fn checkpoint_survives_second_restart_after_wal_replay<FS, Open, OpenFut>(
    mut open: Open,
) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    // First run: write + durable WAL sync.
    {
        let mut db = open().await?;
        db.put(b"k1", b"v1")?;
        db.flush_wal()?;
    }

    // First restart: replay from WAL, then checkpoint to SST and clear WAL.
    {
        let mut db = open().await?;
        assert_eq!(db.get(b"k1")?, Some(b"v1".to_vec()));
        db.flush()?;
        assert_eq!(db.debug_state()?.wal_bytes, 0);
    }

    // Second restart: WAL may be empty, so value must come from checkpointed SST+manifest.
    {
        let db = open().await?;
        assert_eq!(db.get(b"k1")?, Some(b"v1".to_vec()));
        assert_eq!(db.debug_state()?.wal_bytes, 0);
    }

    Ok(())
}

pub async fn unknown_merge_operator_rejected_on_open<
    FS,
    OpenWith,
    OpenWithFut,
    OpenWithout,
    OpenWithoutFut,
>(
    mut open_with_merge: OpenWith,
    mut open_without_merge: OpenWithout,
) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    OpenWith: FnMut() -> OpenWithFut,
    OpenWithFut: Future<Output = TestResult<LsmTree<FS>>>,
    OpenWithout: FnMut() -> OpenWithoutFut,
    OpenWithoutFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    {
        let mut db = open_with_merge().await?;
        db.merge(b"k", 1, b"a")?;
        db.flush_wal()?;
    }

    let result = open_without_merge().await;
    assert!(result.is_err());

    Ok(())
}

pub async fn delete_dominates_merge_history<FS, Open, OpenFut>(mut open: Open) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    {
        let mut db = open().await?;
        db.put(b"k", b"base")?;
        db.merge(b"k", 1, b"-1")?;
        db.delete(b"k")?;
        db.merge(b"k", 1, b"-2")?;
        db.flush_wal()?;

        assert_eq!(db.get(b"k")?, None);
    }

    {
        let db = open().await?;
        assert_eq!(db.get(b"k")?, None);
    }

    Ok(())
}

pub async fn range_scan_returns_sorted_live_values<FS, Open, OpenFut>(mut open: Open) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    let mut db = open().await?;
    db.put(b"a", b"1")?;
    db.put(b"b", b"2")?;
    db.put(b"c", b"3")?;
    db.delete(b"b")?;

    let rows = db.scan_range(Some(b"a"), Some(b"d"))?;
    assert_eq!(
        rows,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec())
        ]
    );

    Ok(())
}

pub async fn deepest_compaction_drops_safe_tombstones<FS, Open, OpenFut>(
    mut open: Open,
) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    let mut db = open().await?;

    db.put(b"victim", b"v")?;
    db.flush()?;

    db.delete(b"victim")?;
    db.flush()?;

    db.put(b"x1", b"1")?;
    db.flush()?;

    db.put(b"x2", b"2")?;
    db.flush()?;

    while db.compact_step()? {}

    assert_eq!(db.get(b"victim")?, None);
    let state = db.debug_state()?;
    assert_eq!(state.deepest_tombstones, 0);

    Ok(())
}

pub async fn flush_truncates_wal<FS, Open, OpenFut>(mut open: Open) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    let mut db = open().await?;
    db.put(b"k", b"v")?;
    db.flush_wal()?;
    assert!(db.debug_state()?.wal_bytes > 0);

    db.flush()?;
    assert_eq!(db.debug_state()?.wal_bytes, 0);

    Ok(())
}

pub async fn crud_update_delete_range_correctness_large<FS, Open, OpenFut>(
    mut open: Open,
) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    const N: usize = 800;

    let mut db = open().await?;
    let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

    for i in 0..N {
        let k = key(i);
        let v = val("insert", i);
        db.put(&k, &v)?;
        model.insert(k, Some(v));
    }

    for i in (0..N).step_by(3) {
        let k = key(i);
        let v = val("update", i);
        db.put(&k, &v)?;
        model.insert(k, Some(v));
    }

    for i in (0..N).step_by(5) {
        let k = key(i);
        db.delete(&k)?;
        model.insert(k, None);
    }

    // Spot check every key via point reads.
    for i in 0..N {
        let k = key(i);
        let got = db.get(&k)?;
        let expected = model.get(&k).cloned().unwrap_or(None);
        assert_eq!(got, expected, "mismatch for key {i}");
    }

    // Validate range scan semantics over a sub-range.
    let start = key(120);
    let end = key(680);
    let rows = db.scan_range(Some(&start), Some(&end))?;
    let expected_rows: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .filter_map(|(k, v)| {
            if k.as_slice() >= start.as_slice() && k.as_slice() < end.as_slice() {
                v.clone().map(|vv| (k.clone(), vv))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(rows, expected_rows);

    db.flush()?;
    Ok(())
}

pub async fn merge_and_multi_tier_correctness_large<FS, Open, OpenFut>(mut open: Open) -> TestResult
where
    FS: SyncFs + Clone + 'static,
    Open: FnMut() -> OpenFut,
    OpenFut: Future<Output = TestResult<LsmTree<FS>>>,
{
    const N: usize = 1500;

    let mut db = open().await?;
    let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

    // Seed base values.
    for i in 0..N {
        let k = key(i);
        let v = val("base", i);
        db.put(&k, &v)?;
        model.insert(k, Some(v));
    }

    // Merge operands on all keys.
    for i in 0..N {
        let k = key(i);
        let op = format!("|m{:03}|", i % 11).into_bytes();
        db.merge(&k, 1, &op)?;
        let next = append_merge(model.get(&k).and_then(|v| v.as_deref()), &op);
        model.insert(k, Some(next));
    }

    // More updates / deletes to mix version histories.
    for i in (0..N).step_by(7) {
        let k = key(i);
        let v = val("overwrite", i);
        db.put(&k, &v)?;
        model.insert(k, Some(v));
    }
    for i in (0..N).step_by(9) {
        let k = key(i);
        db.delete(&k)?;
        model.insert(k, None);
    }
    for i in (0..N).step_by(13) {
        let k = key(i);
        let op = b"|tail|".to_vec();
        db.merge(&k, 1, &op)?;
        let next = append_merge(model.get(&k).and_then(|v| v.as_deref()), &op);
        // Delete still dominates only when delete is newer; this merge is newest for this key.
        model.insert(k, Some(next));
    }

    db.flush()?;
    while db.compact_step()? {}

    // Full point-check correctness.
    for i in 0..N {
        let k = key(i);
        let got = db.get(&k)?;
        let expected = model.get(&k).cloned().unwrap_or(None);
        assert_eq!(got, expected, "merge mismatch for key {i}");
    }

    // Full ordered range scan correctness.
    let rows = db.scan_range(None, None)?;
    let expected_rows: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .filter_map(|(k, v)| v.clone().map(|vv| (k.clone(), vv)))
        .collect();
    assert_eq!(rows, expected_rows);

    // Ensure we exercised multiple levels/tier movement.
    let state = db.debug_state()?;
    assert!(
        state.level_file_counts.iter().skip(1).any(|&n| n > 0),
        "expected data beyond L0, got {:?}",
        state.level_file_counts
    );

    Ok(())
}
