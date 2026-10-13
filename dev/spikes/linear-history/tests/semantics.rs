//! Black-box semantics of the linear-history spike through its public
//! `Authority` / `Client` API, on both Memory and RocksDB storage.

use groove::storage::{BoxedStorage, MemoryStorage};
use jazz_storage_rocksdb::RocksDbStorage;
use linear_history::codec::COLUMN_FAMILIES;
use linear_history::merges::{
    counter_i64, i64_value, u64_set, u64_set_value, value_i64, value_u64_set,
};
use linear_history::*;

const TASKS: TableId = 1;
const TITLE: usize = 0;
const DONE: usize = 1;
const COUNT: usize = 2;
const TAGS: usize = 3;

fn schema() -> Schema {
    Schema::new([TableDef::new(TASKS)
        .column("title")
        .indexed_column("done")
        .merged_column("count", counter_i64)
        .merged_column("tags", u64_set)])
}

fn v(s: &str) -> Value {
    Some(s.as_bytes().to_vec())
}

fn for_each_backend(options: AuthorityOptions, test: impl Fn(Authority<BoxedStorage>)) {
    let memory = BoxedStorage::new(MemoryStorage::new(&COLUMN_FAMILIES).unwrap());
    test(Authority::open(memory, schema(), options).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let rocks = BoxedStorage::new(RocksDbStorage::open(dir.path(), &COLUMN_FAMILIES).unwrap());
    test(Authority::open(rocks, schema(), options).unwrap());
}

fn both_layouts(test: impl Fn(Authority<BoxedStorage>)) {
    for separate_current in [true, false] {
        for_each_backend(AuthorityOptions { separate_current }, &test);
    }
}

fn sync(client: &mut Client, core: &Authority<impl groove::storage::OrderedKvStorage>, since: Seq) {
    client.receive(TASKS, core.changes_since(TASKS, since).unwrap());
}

fn accept(
    core: &mut Authority<impl groove::storage::OrderedKvStorage>,
    client: &mut Client,
    tx: &Tx,
) -> Seq {
    let outcome = core.apply(tx).unwrap();
    client.settle(tx.id);
    match outcome {
        Outcome::Accepted(seq) => seq,
        rejected => panic!("expected acceptance, got {rejected:?}"),
    }
}

#[test]
fn concurrent_lww_columns_merge_per_column_without_parents() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let mut b = Client::new(2, schema());
        let seed = a
            .begin()
            .set(TASKS, 7, TITLE, v("draft"))
            .set(TASKS, 7, DONE, v("no"))
            .commit();
        accept(&mut core, &mut a, &seed);
        sync(&mut a, &core, 0);
        sync(&mut b, &core, 0);

        let from_a = a.begin().set(TASKS, 7, TITLE, v("final")).commit();
        let from_b = b.begin().set(TASKS, 7, DONE, v("yes")).commit();
        accept(&mut core, &mut b, &from_b);
        accept(&mut core, &mut a, &from_a);

        let row = core.get(TASKS, 7).unwrap().unwrap();
        assert_eq!(row.value(TITLE), Some(&b"final"[..]));
        assert_eq!(row.value(DONE), Some(&b"yes"[..]));
    });
}

#[test]
fn stale_lww_write_loses_to_newer_stamp_regardless_of_arrival() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let mut b = Client::new(2, schema());
        let old = a.begin().set(TASKS, 1, TITLE, v("old")).commit();
        // b's clock is ahead, so its write carries the newer stamp.
        let _ = b.begin().set(TASKS, 99, TITLE, v("warmup")).commit();
        let new = b.begin().set(TASKS, 1, TITLE, v("new")).commit();
        accept(&mut core, &mut b, &new);
        accept(&mut core, &mut a, &old);
        assert_eq!(
            core.get(TASKS, 1).unwrap().unwrap().value(TITLE),
            Some(&b"new"[..])
        );
    });
}

#[test]
fn three_way_counter_sums_concurrent_increments_from_history_bases() {
    both_layouts(|mut core| {
        let mut clients: Vec<Client> = (1..=3).map(|n| Client::new(n, schema())).collect();
        let seed = clients[0]
            .begin()
            .set(TASKS, 5, COUNT, i64_value(10))
            .commit();
        accept(&mut core, &mut clients[0], &seed);
        for c in clients.iter_mut() {
            sync(c, &core, 0);
        }
        // Each client increments from the same stale base (10).
        let txs: Vec<Tx> = clients
            .iter_mut()
            .enumerate()
            .map(|(i, c)| {
                let seen = value_i64(c.view(TASKS, 5).unwrap().value(COUNT));
                c.begin()
                    .merge(TASKS, 5, COUNT, i64_value(seen + i as i64 + 1))
                    .commit()
            })
            .collect();
        for (c, tx) in clients.iter_mut().zip(&txs) {
            assert!(matches!(
                tx.writes[0].cells[0].1,
                CellWrite::ThreeWay {
                    base: BaseRef::AtSeq(1),
                    ..
                }
            ));
            accept(&mut core, c, tx);
        }
        assert_eq!(
            value_i64(core.get(TASKS, 5).unwrap().unwrap().value(COUNT)),
            16
        );
    });
}

#[test]
fn chained_pending_three_way_writes_ship_inline_bases_and_do_not_double_count() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let mut b = Client::new(2, schema());
        let seed = a.begin().set(TASKS, 5, COUNT, i64_value(10)).commit();
        accept(&mut core, &mut a, &seed);
        sync(&mut a, &core, 0);
        sync(&mut b, &core, 0);

        // a: 10 -> 11 -> 12 offline; b: +5 concurrently.
        let a1 = a.begin().merge(TASKS, 5, COUNT, i64_value(11)).commit();
        let a2 = a.begin().merge(TASKS, 5, COUNT, i64_value(12)).commit();
        assert_eq!(value_i64(a.view(TASKS, 5).unwrap().value(COUNT)), 12);
        assert!(matches!(
            &a2.writes[0].cells[0].1,
            CellWrite::ThreeWay {
                base: BaseRef::Inline(Some(_)),
                ..
            }
        ));
        let b1 = b.begin().merge(TASKS, 5, COUNT, i64_value(15)).commit();

        accept(&mut core, &mut b, &b1);
        accept(&mut core, &mut a, &a1);
        accept(&mut core, &mut a, &a2);
        assert_eq!(
            value_i64(core.get(TASKS, 5).unwrap().unwrap().value(COUNT)),
            17
        );
    });
}

#[test]
fn three_way_set_merge_keeps_concurrent_adds_and_removals() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let mut b = Client::new(2, schema());
        let seed = a
            .begin()
            .set(TASKS, 3, TAGS, u64_set_value([1, 2, 3]))
            .commit();
        accept(&mut core, &mut a, &seed);
        sync(&mut a, &core, 0);
        sync(&mut b, &core, 0);
        let remove_two = a
            .begin()
            .merge(TASKS, 3, TAGS, u64_set_value([1, 3]))
            .commit();
        let add_four = b
            .begin()
            .merge(TASKS, 3, TAGS, u64_set_value([1, 2, 3, 4]))
            .commit();
        accept(&mut core, &mut a, &remove_two);
        accept(&mut core, &mut b, &add_four);
        assert_eq!(
            value_u64_set(core.get(TASKS, 3).unwrap().unwrap().value(TAGS)),
            vec![1, 3, 4]
        );
    });
}

#[test]
fn rejection_drops_only_that_pending_transaction_without_cascade() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let seed = a.begin().set(TASKS, 1, TITLE, v("x")).commit();
        let base = accept(&mut core, &mut a, &seed);
        sync(&mut a, &core, 0);

        // A concurrent writer invalidates a's exclusive read.
        let mut b = Client::new(2, schema());
        let bump = b.begin().set(TASKS, 1, TITLE, v("y")).commit();
        accept(&mut core, &mut b, &bump);

        let doomed = a
            .begin()
            .set(TASKS, 2, TITLE, v("needs-1-unchanged"))
            .commit_as(TxKind::Exclusive {
                base,
                rows_read: vec![(TASKS, 1)],
                predicates: vec![],
            });
        let follower = a.begin().set(TASKS, 2, DONE, v("yes")).commit();
        assert_eq!(
            core.apply(&doomed).unwrap(),
            Outcome::Rejected(RejectReason::RowConflict)
        );
        a.settle(doomed.id);
        assert_eq!(a.view(TASKS, 2).unwrap().value(TITLE), None);
        assert_eq!(a.view(TASKS, 2).unwrap().value(DONE), Some(&b"yes"[..]));
        accept(&mut core, &mut a, &follower);
        let row = core.get(TASKS, 2).unwrap().unwrap();
        assert_eq!(
            (row.value(TITLE), row.value(DONE)),
            (None, Some(&b"yes"[..]))
        );
    });
}

#[test]
fn exclusive_predicate_detects_phantoms_from_the_change_log() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let seed = a
            .begin()
            .set(TASKS, 1, DONE, v("no"))
            .set(TASKS, 2, DONE, v("yes"))
            .commit();
        let base = accept(&mut core, &mut a, &seed);
        let open_tasks = EqPredicate {
            table: TASKS,
            column: DONE,
            value: v("no"),
        };
        let exclusive = |counter_row: RowId, a: &mut Client| {
            a.begin()
                .set(TASKS, counter_row, TITLE, v("summary"))
                .commit_as(TxKind::Exclusive {
                    base,
                    rows_read: vec![],
                    predicates: vec![open_tasks.clone()],
                })
        };

        // A change outside the predicate does not conflict.
        let mut b = Client::new(2, schema());
        let unrelated = b.begin().set(TASKS, 2, TITLE, v("renamed")).commit();
        accept(&mut core, &mut b, &unrelated);
        let ok = exclusive(100, &mut a);
        assert!(matches!(core.apply(&ok).unwrap(), Outcome::Accepted(_)));

        // A row entering the predicate (phantom) conflicts.
        let phantom = b.begin().set(TASKS, 3, DONE, v("no")).commit();
        accept(&mut core, &mut b, &phantom);
        let conflicted = exclusive(101, &mut a);
        assert_eq!(
            core.apply(&conflicted).unwrap(),
            Outcome::Rejected(RejectReason::PredicateConflict)
        );
    });
}

#[test]
fn snapshot_reads_and_both_query_strategies_agree_at_every_cut() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let mut cuts = vec![0];
        let seed = (0..20u128)
            .fold(a.begin(), |t, row| t.set(TASKS, row, DONE, v("no")))
            .commit();
        cuts.push(accept(&mut core, &mut a, &seed));
        for round in 0..5u128 {
            let mut t = a.begin();
            for row in (round..20).step_by(3) {
                t = t.set(
                    TASKS,
                    row,
                    DONE,
                    v(if round % 2 == 0 { "yes" } else { "no" }),
                );
            }
            t = t.delete(TASKS, round * 4, true);
            let tx = t.commit();
            cuts.push(accept(&mut core, &mut a, &tx));
        }
        let now = core.last_seq();
        for &cut in &cuts {
            let visible = core.scan_at(TASKS, cut).unwrap();
            // Point reads agree with the scan.
            for (row, image) in &visible {
                assert_eq!(core.get_at(TASKS, *row, cut).unwrap().as_ref(), Some(image));
            }
            let expected: std::collections::BTreeSet<RowId> = visible
                .iter()
                .filter(|(_, i)| i.value(DONE) == Some(&b"yes"[..]))
                .map(|(r, _)| *r)
                .collect();
            for strategy in [SnapshotStrategy::Forward, SnapshotStrategy::Rewind] {
                assert_eq!(
                    core.lookup_eq_at(TASKS, DONE, b"yes", cut, strategy)
                        .unwrap(),
                    expected,
                    "cut {cut} {strategy:?}"
                );
            }
        }
        assert_eq!(
            core.lookup_eq(TASKS, DONE, b"yes").unwrap(),
            core.lookup_eq_at(TASKS, DONE, b"yes", now, SnapshotStrategy::Forward)
                .unwrap()
        );
    });
}

#[test]
fn content_write_does_not_restore_a_deleted_row() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let mut b = Client::new(2, schema());
        let seed = a.begin().set(TASKS, 1, TITLE, v("x")).commit();
        accept(&mut core, &mut a, &seed);
        let deletion = a.begin().delete(TASKS, 1, true).commit();
        let edit = b.begin().set(TASKS, 1, TITLE, v("edited offline")).commit();
        accept(&mut core, &mut a, &deletion);
        accept(&mut core, &mut b, &edit);
        let row = core.get(TASKS, 1).unwrap().unwrap();
        assert!(row.deleted);
        assert_eq!(row.value(TITLE), Some(&b"edited offline"[..]));
        assert!(core.scan_at(TASKS, core.last_seq()).unwrap().is_empty());
    });
}

#[test]
fn replayed_transaction_is_idempotent() {
    both_layouts(|mut core| {
        let mut a = Client::new(1, schema());
        let seed = a.begin().set(TASKS, 5, COUNT, i64_value(1)).commit();
        accept(&mut core, &mut a, &seed);
        sync(&mut a, &core, 0);
        let inc = a.begin().merge(TASKS, 5, COUNT, i64_value(2)).commit();
        let first = core.apply(&inc).unwrap();
        let replay = core.apply(&inc).unwrap();
        assert_eq!(first, replay);
        assert_eq!(
            value_i64(core.get(TASKS, 5).unwrap().unwrap().value(COUNT)),
            2
        );
    });
}
