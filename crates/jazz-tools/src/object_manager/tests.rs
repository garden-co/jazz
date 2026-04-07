use super::*;
use crate::metadata::{MetadataKey, RowProvenance};
use crate::storage::MemoryStorage;

fn row_metadata(table: &str) -> HashMap<String, String> {
    HashMap::from([(MetadataKey::Table.to_string(), table.to_string())])
}

fn visible_row(
    row_id: ObjectId,
    branch: &str,
    parents: Vec<CommitId>,
    timestamp: u64,
    author: ObjectId,
    data: &[u8],
) -> StoredRowVersion {
    let provenance = if parents.is_empty() {
        RowProvenance::for_insert(author.to_string(), timestamp)
    } else {
        RowProvenance {
            created_by: author.to_string(),
            created_at: 1_000,
            updated_by: author.to_string(),
            updated_at: timestamp,
        }
    };

    StoredRowVersion::new(
        row_id,
        branch,
        parents,
        data.to_vec(),
        provenance,
        HashMap::new(),
        RowState::VisibleDirect,
        None,
    )
}

#[test]
fn create_object_with_metadata() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();

    let id = manager.create(&mut io, Some(row_metadata("users")));

    let object = manager.get(id).expect("object should exist");
    assert_eq!(
        object.metadata.get(MetadataKey::Table.as_str()),
        Some(&"users".to_string())
    );
}

#[test]
fn add_row_version_rejects_unknown_object() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = ObjectId::new();
    let author = ObjectId::new();

    let result = manager.add_row_version(
        &mut io,
        row_id,
        "main",
        visible_row(row_id, "main", Vec::new(), 1_000, author, b"alice"),
    );

    assert_eq!(result, Err(Error::ObjectNotFound(row_id)));
}

#[test]
fn add_row_version_rejects_unknown_parent() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = manager.create(&mut io, Some(row_metadata("users")));
    let author = ObjectId::new();
    let missing_parent = CommitId([7; 32]);

    let result = manager.add_row_version(
        &mut io,
        row_id,
        "main",
        visible_row(
            row_id,
            "main",
            vec![missing_parent],
            2_000,
            author,
            b"alice-v2",
        ),
    );

    assert_eq!(result, Err(Error::BranchNotFound(BranchName::new("main"))));

    manager
        .add_row_version(
            &mut io,
            row_id,
            "main",
            visible_row(row_id, "main", Vec::new(), 1_000, author, b"alice-v1"),
        )
        .unwrap();

    let result = manager.add_row_version(
        &mut io,
        row_id,
        "main",
        visible_row(
            row_id,
            "main",
            vec![missing_parent],
            2_000,
            author,
            b"alice-v2",
        ),
    );

    assert_eq!(result, Err(Error::ParentNotFound(missing_parent)));
}

#[test]
fn add_row_version_tracks_visible_row_and_tips() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = manager.create(&mut io, Some(row_metadata("users")));
    let author = ObjectId::new();

    let root = visible_row(row_id, "main", Vec::new(), 1_000, author, b"root");
    let root_id = manager
        .add_row_version(&mut io, row_id, "main", root.clone())
        .unwrap();

    let alice = visible_row(row_id, "main", vec![root_id], 2_000, author, b"alice");
    let alice_id = manager
        .add_row_version(&mut io, row_id, "main", alice.clone())
        .unwrap();

    let bob = visible_row(row_id, "main", vec![root_id], 3_000, author, b"bob");
    let bob_id = manager
        .add_row_version(&mut io, row_id, "main", bob.clone())
        .unwrap();

    let tips = manager.get_tip_ids(row_id, "main").unwrap();
    assert_eq!(tips.len(), 2);
    assert!(tips.contains(&alice_id));
    assert!(tips.contains(&bob_id));

    let winner = manager
        .visible_row(row_id, BranchName::new("main"))
        .expect("main branch should have a visible winner");
    assert_eq!(winner.version_id(), bob_id);
    assert_eq!(winner.data, b"bob".to_vec());

    let updates = manager.take_row_object_updates();
    assert_eq!(updates.len(), 3);
    assert_eq!(updates[0].row.version_id(), root_id);
    assert_eq!(updates[1].row.version_id(), alice_id);
    assert_eq!(updates[2].row.version_id(), bob_id);
}

#[test]
fn stale_row_version_does_not_replace_visible_winner() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = manager.create(&mut io, Some(row_metadata("users")));
    let author = ObjectId::new();

    let newer = visible_row(row_id, "main", Vec::new(), 2_000, author, b"newer");
    let newer_id = manager
        .add_row_version(&mut io, row_id, "main", newer.clone())
        .unwrap();
    let _ = manager.take_row_object_updates();

    let older = visible_row(row_id, "main", Vec::new(), 1_000, author, b"older");
    let older_id = manager
        .add_row_version(&mut io, row_id, "main", older.clone())
        .unwrap();

    assert_ne!(newer_id, older_id);
    assert!(
        manager.take_row_object_updates().is_empty(),
        "stale history should not emit a visible-row update"
    );
    assert_eq!(
        manager
            .visible_row(row_id, BranchName::new("main"))
            .expect("main branch should still have a visible row")
            .version_id(),
        newer_id
    );
}

#[test]
fn patch_row_version_state_promotes_confirmed_tier_monotonically() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = manager.create(&mut io, Some(row_metadata("users")));
    let author = ObjectId::new();

    let row = visible_row(row_id, "main", Vec::new(), 1_000, author, b"alice");
    let version_id = manager
        .add_row_version(&mut io, row_id, "main", row)
        .unwrap();
    let _ = manager.take_row_object_updates();

    let update = manager
        .patch_row_version_state(
            row_id,
            &BranchName::new("main"),
            version_id,
            None,
            Some(DurabilityTier::EdgeServer),
        )
        .expect("durability changes should surface as row updates for the visible winner");
    assert_eq!(update.row.version_id(), version_id);
    assert_eq!(update.row.confirmed_tier, Some(DurabilityTier::EdgeServer));

    manager.patch_row_version_state(
        row_id,
        &BranchName::new("main"),
        version_id,
        None,
        Some(DurabilityTier::Worker),
    );

    let visible = manager
        .visible_row(row_id, BranchName::new("main"))
        .expect("visible row should remain present");
    assert_eq!(visible.confirmed_tier, Some(DurabilityTier::EdgeServer));
}

#[test]
fn get_or_load_hydrates_visible_and_history_rows_from_storage() {
    let mut writer_storage = MemoryStorage::new();
    let mut writer = ObjectManager::new();
    let row_id = writer.create(&mut writer_storage, Some(row_metadata("users")));
    let author = ObjectId::new();

    let older = visible_row(row_id, "main", Vec::new(), 1_000, author, b"older");
    let older_id = writer
        .add_row_version(&mut writer_storage, row_id, "main", older)
        .unwrap();
    let newer = visible_row(row_id, "main", vec![older_id], 2_000, author, b"newer");
    let newer_id = writer
        .add_row_version(&mut writer_storage, row_id, "main", newer)
        .unwrap();

    let mut reader = ObjectManager::new();
    let loaded = reader
        .get_or_load(row_id, &writer_storage, &["main".to_string()])
        .expect("row object should load from storage");

    assert_eq!(
        loaded.metadata.get(MetadataKey::Table.as_str()),
        Some(&"users".to_string())
    );
    assert_eq!(
        reader
            .visible_row(row_id, BranchName::new("main"))
            .expect("visible row should hydrate from visible region")
            .version_id(),
        newer_id
    );
    let tips = reader.get_tip_ids(row_id, "main").unwrap();
    assert_eq!(tips.len(), 1);
    assert!(tips.contains(&newer_id));
}
