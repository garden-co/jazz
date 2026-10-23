//! Serializable aggregate reads are recorded and validated by their result
//! (garden-co/jazz#3768).

use super::execution::commit_metric_global_to_authority;
use super::*;
use crate::node::aggregate_read_result::decode_aggregate_read_result;
use crate::tx::{AggregateReadResult, Transaction};

struct Peers {
    /// Kept alive for the nodes' lifetime: destructure with `_dirs`, not
    /// `..`, or the stores are deleted while still open.
    _dirs: [tempfile::TempDir; 3],
    alice: NodeState,
    bob: NodeState,
    authority: NodeState,
}

/// Alice and Bob write `metrics` through one authority. Every seeded row is
/// globally accepted and known to Alice.
fn peers(seed: &[(usize, &str, i64)]) -> Peers {
    let schema = signed_metric_schema();
    let (alice_dir, mut alice) =
        open_node_with_uuid(NodeUuid::from_bytes([0xb1; 16]), schema.clone());
    let (bob_dir, bob) = open_node_with_uuid(NodeUuid::from_bytes([0xb2; 16]), schema.clone());
    let (authority_dir, mut authority) =
        open_node_with_uuid(NodeUuid::from_bytes([0xb9; 16]), schema);
    for (offset, (idx, bucket, score)) in seed.iter().enumerate() {
        commit_metric_global_to_authority(
            &mut alice,
            &mut authority,
            row(*idx),
            bucket,
            *score,
            1_000 + offset as u64,
        );
    }
    Peers {
        _dirs: [alice_dir, bob_dir, authority_dir],
        alice,
        bob,
        authority,
    }
}

fn shape_of(query: Query) -> (ValidatedQuery, Binding) {
    let shape = query.validate(&signed_metric_schema()).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn count_in(bucket: &str) -> (ValidatedQuery, Binding) {
    shape_of(
        Query::from("metrics")
            .filter(eq(col("bucket"), lit(bucket)))
            .count(),
    )
}

/// Alice reads `shape` in a serializable transaction, inserts `row(new_row)`
/// into bucket `a`, and commits. Returns the commit unit she would send.
fn alice_reads_then_inserts(
    alice: &mut NodeState,
    shape: &ValidatedQuery,
    binding: &Binding,
    new_row: usize,
    now_ms: u64,
) -> SyncMessage {
    let tx = OpenTransactionId::new();
    alice.open_serializable(tx).unwrap();
    alice.tx_query(tx, shape, binding).unwrap();
    alice
        .tx_write(
            tx,
            "metrics",
            row(new_row),
            BTreeMap::from([
                ("bucket".to_owned(), Value::String("a".to_owned())),
                ("score".to_owned(), Value::I64(0)),
            ]),
            None,
        )
        .unwrap();
    alice
        .commit_serializable_settled(tx, AuthorSubject::SYSTEM, now_ms)
        .unwrap()
        .1
}

fn authority_fate(authority: &mut NodeState, unit: SyncMessage) -> Fate {
    let [update] = authority
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = update else {
        panic!("expected a fate update, got {update:?}");
    };
    fate
}

fn commit_unit_tx(unit: &SyncMessage) -> &Transaction {
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("expected a commit unit, got {unit:?}");
    };
    tx
}

/// A count stays valid while the number it observed holds: Bob rewrites a
/// row Alice counted without changing the count, and Alice's commit is
/// accepted. Before #3768 the rewrite conflicted because the count was
/// validated through the version of every row it consumed.
///
/// ```text
/// alice ──count(bucket a) = 2──insert──commit─────────────► authority ✓
/// bob   ────────────rewrite counted row (still bucket a)──► authority
/// ```
#[test]
fn serializable_count_accepts_a_rewrite_that_keeps_the_count() {
    let Peers {
        mut alice,
        mut bob,
        mut authority,
        _dirs,
        ..
    } = peers(&[(1, "a", 1), (2, "a", 2), (3, "b", 10)]);
    let (shape, binding) = count_in("a");
    let tx = OpenTransactionId::new();
    alice.open_serializable(tx).unwrap();
    let observed = alice.tx_query(tx, &shape, &binding).unwrap();
    assert_eq!(
        observed[0].test_cells_by_descriptor()["count"],
        Value::U64(2)
    );
    commit_metric_global_to_authority(&mut bob, &mut authority, row(2), "a", 20, 1_010);
    alice
        .tx_write(
            tx,
            "metrics",
            row(4),
            BTreeMap::from([
                ("bucket".to_owned(), Value::String("a".to_owned())),
                ("score".to_owned(), Value::I64(0)),
            ]),
            None,
        )
        .unwrap();
    let (_tx_id, unit) = alice
        .commit_serializable_settled(tx, AuthorSubject::SYSTEM, 1_011)
        .unwrap();
    assert_eq!(authority_fate(&mut authority, unit), Fate::Accepted);
}

/// A count conflicts when its number changes: Bob inserts a row into the
/// bucket Alice counted, so her count of 2 is 3 at commit.
///
/// ```text
/// alice ──count(bucket a) = 2──insert──commit──► authority ✗ SerializationConflict
/// bob   ────────insert into bucket a──────────► authority
/// ```
#[test]
fn serializable_count_conflicts_when_another_writer_adds_a_counted_row() {
    let Peers {
        mut alice,
        mut bob,
        mut authority,
        _dirs,
        ..
    } = peers(&[(1, "a", 1), (2, "a", 2), (3, "b", 10)]);
    let (shape, binding) = count_in("a");
    let tx = OpenTransactionId::new();
    alice.open_serializable(tx).unwrap();
    alice.tx_query(tx, &shape, &binding).unwrap();
    commit_metric_global_to_authority(&mut bob, &mut authority, row(5), "a", 7, 1_010);
    let (_tx_id, unit) = alice
        .commit_serializable_settled(tx, AuthorSubject::SYSTEM, 1_011)
        .unwrap();
    assert_eq!(
        authority_fate(&mut authority, unit),
        Fate::Rejected(RejectionReason::SerializationConflict)
    );

    // A row moving into another bucket changes the count too.
    let (shape, binding) = count_in("b");
    let tx = OpenTransactionId::new();
    alice.open_serializable(tx).unwrap();
    alice.tx_query(tx, &shape, &binding).unwrap();
    commit_metric_global_to_authority(&mut bob, &mut authority, row(3), "c", 10, 1_012);
    let (_tx_id, unit) = alice
        .commit_serializable_settled(tx, AuthorSubject::SYSTEM, 1_013)
        .unwrap();
    assert_eq!(
        authority_fate(&mut authority, unit),
        Fate::Rejected(RejectionReason::SerializationConflict)
    );
}

/// Grouped `sum`, `min` and `max` are validated by their values: Bob swaps
/// two scores inside bucket `a`, which keeps every aggregate, and Alice's
/// commit is accepted; then Bob raises the maximum, and her next commit
/// conflicts.
///
/// ```text
/// alice ──sum/min/max by bucket──insert──commit──► authority ✓
/// bob   ──swap scores 2↔3 in bucket a──────────► authority
/// alice ──sum/min/max by bucket──insert──commit──► authority ✗
/// bob   ──raise max of bucket a────────────────► authority
/// ```
#[test]
fn serializable_grouped_sum_min_max_conflict_only_when_a_value_changes() {
    let Peers {
        mut alice,
        mut bob,
        mut authority,
        _dirs,
        ..
    } = peers(&[(1, "a", 1), (2, "a", 2), (3, "a", 3), (4, "b", 10)]);
    let (shape, binding) = shape_of(
        Query::from("metrics")
            .aggregate([
                Aggregate::sum("score"),
                Aggregate::min("score"),
                Aggregate::max("score"),
            ])
            .group_by("bucket"),
    );

    let tx = OpenTransactionId::new();
    alice.open_serializable(tx).unwrap();
    assert_eq!(alice.tx_query(tx, &shape, &binding).unwrap().len(), 2);
    commit_metric_global_to_authority(&mut bob, &mut authority, row(2), "a", 3, 1_010);
    commit_metric_global_to_authority(&mut bob, &mut authority, row(3), "a", 2, 1_011);
    let (_tx_id, unit) = alice
        .commit_serializable_settled(tx, AuthorSubject::SYSTEM, 1_012)
        .unwrap();
    assert_eq!(authority_fate(&mut authority, unit), Fate::Accepted);

    let unit = {
        let tx = OpenTransactionId::new();
        alice.open_serializable(tx).unwrap();
        alice.tx_query(tx, &shape, &binding).unwrap();
        commit_metric_global_to_authority(&mut bob, &mut authority, row(1), "a", 9, 1_013);
        alice
            .commit_serializable_settled(tx, AuthorSubject::SYSTEM, 1_014)
            .unwrap()
            .1
    };
    assert_eq!(
        authority_fate(&mut authority, unit),
        Fate::Rejected(RejectionReason::SerializationConflict)
    );
}

/// The commit payload of a count does not grow with the rows it counted:
/// the read set carries one predicate read with the `count = N` result and
/// no per-row reads. This inspects the commit unit Alice sends, because the
/// payload's contents are the contract and no public read exposes them.
#[test]
fn serializable_count_commit_payload_is_independent_of_the_counted_rows() {
    let encoded_read_sets = |rows: usize| {
        let seed = (1..=rows)
            .map(|idx| (idx, "a", idx as i64))
            .collect::<Vec<_>>();
        let Peers {
            mut alice, _dirs, ..
        } = peers(&seed);
        let (shape, binding) = count_in("a");
        let unit = alice_reads_then_inserts(&mut alice, &shape, &binding, 1_000, 5_000);
        let tx = commit_unit_tx(&unit);
        assert_eq!(tx.row_read_set.as_deref(), Some(&[][..]));
        assert_eq!(tx.absent_read_set.as_deref(), Some(&[][..]));
        let [read] = tx.predicate_read_set.as_deref().unwrap() else {
            panic!("expected exactly the count's predicate read");
        };
        let result = read.aggregate_result.as_ref().expect("count result");
        let groups = decode_aggregate_read_result(result).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 1);
        assert_eq!(groups[0][0].name, "count");
        assert_eq!(
            groups[0][0].value,
            Some((groove::records::ValueType::U64, Value::U64(rows as u64)))
        );
        postcard::to_allocvec(&(
            &tx.row_read_set,
            &tx.absent_read_set,
            &tx.predicate_read_set,
        ))
        .unwrap()
        .len()
    };
    assert_eq!(encoded_read_sets(3), encoded_read_sets(60));
}

/// An authority rejects a serializable aggregate read whose result is missing
/// or garbled as a malformed commit, not as a conflict and never as a
/// silent accept. Mallory strips or corrupts the result of a count before
/// sending; this tampers with the commit unit directly because no public
/// client API can produce such a payload.
#[test]
fn serializable_aggregate_read_without_a_well_formed_result_is_malformed() {
    let tampered = |tamper: fn(&mut Option<AggregateReadResult>)| {
        let Peers {
            alice: mut mallory,
            mut authority,
            _dirs,
            ..
        } = peers(&[(1, "a", 1), (2, "a", 2)]);
        let (shape, binding) = count_in("a");
        let unit = alice_reads_then_inserts(&mut mallory, &shape, &binding, 9, 1_010);
        let SyncMessage::CommitUnit { mut tx, versions } = unit else {
            panic!("expected a commit unit");
        };
        tamper(&mut tx.predicate_read_set.as_mut().unwrap()[0].aggregate_result);
        authority_fate(&mut authority, SyncMessage::CommitUnit { tx, versions })
    };
    let malformed = |fate: Fate| {
        assert!(
            matches!(
                &fate,
                Fate::Rejected(RejectionReason::MalformedCommit(reason))
                    if reason.contains("aggregate")
            ),
            "unexpected fate {fate:?}"
        );
    };
    malformed(tampered(|result| *result = None));
    malformed(tampered(|result| result.as_mut().unwrap().0.push(0)));
    malformed(tampered(|result| result.as_mut().unwrap().0[0] = 2));
}

/// Alice cannot read an aggregate after writing in the same serializable
/// transaction: the authority validates the aggregate over committed state,
/// which her staged write is not part of, so such a result cannot be
/// checked soundly. The read fails with a typed error naming the pattern;
/// the same aggregate read before the write is accepted.
#[test]
fn serializable_aggregate_read_after_a_write_is_unsupported() {
    let Peers {
        mut alice, _dirs, ..
    } = peers(&[(1, "a", 1)]);
    let (shape, binding) = count_in("a");
    let tx = OpenTransactionId::new();
    alice.open_serializable(tx).unwrap();
    alice
        .tx_write(
            tx,
            "metrics",
            row(2),
            BTreeMap::from([
                ("bucket".to_owned(), Value::String("a".to_owned())),
                ("score".to_owned(), Value::I64(2)),
            ]),
            None,
        )
        .unwrap();
    let error = alice.tx_query(tx, &shape, &binding).unwrap_err();
    assert!(
        matches!(&error, Error::UnsupportedSerializableRead(pattern) if pattern.contains("after a write")),
        "unexpected error {error:?}"
    );
}
