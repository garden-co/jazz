//! lowering query-evaluation tests.

use super::*;
#[test]
fn exclusive_join_shape_uses_shared_snapshot_lowering() {
    let schema = schema();
    let (_client_dir, mut client) =
        open_node_with_uuid(NodeUuid::from_bytes([1; 16]), schema.clone());
    let alice = author(1);
    client
        .commit_mergeable_settled(
            MergeableCommit::new("issues", row(1), 10).cells(BTreeMap::from([(
                "title".to_owned(),
                Value::String("issue".to_owned()),
            )])),
        )
        .unwrap();
    client
        .commit_mergeable_settled(MergeableCommit::new("issue_members", row(2), 11).cells(
            BTreeMap::from([
                ("issue".to_owned(), Value::Uuid(row(1).0)),
                ("user".to_owned(), Value::Uuid(alice.test_uuid())),
            ]),
        ))
        .unwrap();

    let shape = Query::from("issues")
        .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();

    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    let rows = client
        .tx_query(open, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(rows, BTreeSet::from([row(1)]));
}
