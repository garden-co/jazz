// Stable wire UUID identity across local alias assignment.

#[test]
fn wire_commit_units_preserve_node_and_schema_uuids_not_local_aliases() {
    let schema = schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x4a), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x4b), schema.clone());
    let (parent, parent_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x4a), 10).cells(title_cells("parent")),
        )
        .unwrap();
    core.apply_sync_message_settled(parent_unit).unwrap();
    let (_child_tx, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x4a), 11)
                .parents(vec![parent])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = &unit else {
        panic!("commit unit expected");
    };
    assert_eq!(tx.tx_id.node, node(0x4a));
    assert_eq!(versions[0].schema_version(), schema.version_id());
    assert_eq!(versions[0].parents(), vec![parent]);
    assert_eq!(versions[0].parents()[0].node, node(0x4a));

    core.apply_sync_message_settled(unit).unwrap();
    assert_ne!(
        writer.node_aliases[&node(0x4a)],
        core.node_aliases[&node(0x4a)],
        "replicas deliberately compress the same wire node UUID with independent local aliases"
    );
    let stored = core.query_table_versions("todos").unwrap();
    let child_row = stored
        .iter()
        .find(|version| version.parents().contains(&parent))
        .unwrap();
    let stored_wire = core.version_record_from_row(child_row).unwrap();
    assert_eq!(stored_wire.schema_version(), schema.version_id());
}
use crate::node::query_engine::QueryAuthorizationMode;
