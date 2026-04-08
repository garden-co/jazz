use super::*;
use crate::metadata::{MetadataKey, RowProvenance};
use crate::storage::{MemoryStorage, Storage};

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

fn load_visible_row(storage: &MemoryStorage, row_id: ObjectId, branch: &str) -> StoredRowVersion {
    storage
        .load_visible_region_row("users", branch, row_id)
        .unwrap()
        .expect("visible row should exist")
}

#[test]
fn create_object_with_metadata() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();

    let id = manager.create(&mut io, Some(row_metadata("users")));

    let object = manager.get(id).expect("object metadata should exist");
    assert_eq!(
        object.get(MetadataKey::Table.as_str()),
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

    assert_eq!(result, Err(Error::ParentNotFound(missing_parent)));

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
    let root = manager
        .add_row_version_with_update(&mut io, row_id, "main", root.clone())
        .unwrap();
    let root_id = root.version_id;

    let alice = visible_row(row_id, "main", vec![root_id], 2_000, author, b"alice");
    let alice = manager
        .add_row_version_with_update(&mut io, row_id, "main", alice.clone())
        .unwrap();
    let alice_id = alice.version_id;

    let bob = visible_row(row_id, "main", vec![root_id], 3_000, author, b"bob");
    let bob = manager
        .add_row_version_with_update(&mut io, row_id, "main", bob.clone())
        .unwrap();
    let bob_id = bob.version_id;

    let tips = manager.get_tip_ids(row_id, "main").unwrap();
    assert_eq!(tips.len(), 2);
    assert!(tips.contains(&alice_id));
    assert!(tips.contains(&bob_id));

    let winner = load_visible_row(&io, row_id, "main");
    assert_eq!(winner.version_id(), bob_id);
    assert_eq!(winner.data, b"bob".to_vec());

    assert_eq!(
        root.visible_update
            .as_ref()
            .map(|update| update.row.version_id()),
        Some(root_id)
    );
    assert_eq!(
        alice
            .visible_update
            .as_ref()
            .map(|update| update.row.version_id()),
        Some(alice_id)
    );
    assert_eq!(
        bob.visible_update
            .as_ref()
            .map(|update| update.row.version_id()),
        Some(bob_id)
    );
}

#[test]
fn add_row_version_keeps_distinct_same_timestamp_siblings_in_history() {
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

    let bob = visible_row(row_id, "main", vec![root_id], 2_000, author, b"bob");
    let bob_id = manager
        .add_row_version(&mut io, row_id, "main", bob.clone())
        .unwrap();

    assert_ne!(alice_id, bob_id);

    let history_rows = io.scan_history_row_versions("users", row_id).unwrap();
    assert_eq!(history_rows.len(), 3, "history should keep both siblings");

    let tips = manager.get_tip_ids(row_id, "main").unwrap();
    assert_eq!(tips.len(), 2);
    assert!(tips.contains(&alice_id));
    assert!(tips.contains(&bob_id));
}

#[test]
fn add_row_version_keeps_identical_payload_versions_across_branches() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = manager.create(&mut io, Some(row_metadata("users")));
    let author = ObjectId::new();

    let main = visible_row(row_id, "main", Vec::new(), 1_000, author, b"same");
    let main_id = manager
        .add_row_version(&mut io, row_id, "main", main.clone())
        .unwrap();

    let draft = visible_row(row_id, "draft", Vec::new(), 1_000, author, b"same");
    let draft_id = manager
        .add_row_version(&mut io, row_id, "draft", draft.clone())
        .unwrap();

    assert_ne!(main_id, draft_id, "branch-local versions must not collide");

    let history_rows = io.scan_history_row_versions("users", row_id).unwrap();
    assert_eq!(history_rows.len(), 2, "history should retain both branches");
    assert_eq!(load_visible_row(&io, row_id, "main").data, b"same".to_vec());
    assert_eq!(
        load_visible_row(&io, row_id, "draft").data,
        b"same".to_vec()
    );
}

#[test]
fn stale_row_version_does_not_replace_visible_winner() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = manager.create(&mut io, Some(row_metadata("users")));
    let author = ObjectId::new();

    let newer = visible_row(row_id, "main", Vec::new(), 2_000, author, b"newer");
    let newer = manager
        .add_row_version_with_update(&mut io, row_id, "main", newer.clone())
        .unwrap();
    let newer_id = newer.version_id;

    let older = visible_row(row_id, "main", Vec::new(), 1_000, author, b"older");
    let older = manager
        .add_row_version_with_update(&mut io, row_id, "main", older.clone())
        .unwrap();
    let older_id = older.version_id;

    assert_ne!(newer_id, older_id);
    assert!(
        older.visible_update.is_none(),
        "stale history should not emit a visible-row update"
    );
    assert_eq!(load_visible_row(&io, row_id, "main").version_id(), newer_id);
}

#[test]
fn out_of_order_global_row_updates_tier_pointer_without_becoming_current() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let row_id = manager.create(&mut io, Some(row_metadata("users")));
    let author = ObjectId::new();

    let mut newer = visible_row(row_id, "main", Vec::new(), 2_000, author, b"newer");
    newer.confirmed_tier = Some(DurabilityTier::Worker);
    let newer_id = manager
        .add_row_version(&mut io, row_id, "main", newer)
        .unwrap();

    let mut older = visible_row(row_id, "main", Vec::new(), 1_000, author, b"older");
    older.confirmed_tier = Some(DurabilityTier::GlobalServer);
    let older_id = manager
        .add_row_version(&mut io, row_id, "main", older)
        .unwrap();

    let visible = io
        .load_visible_region_entry("users", "main", row_id)
        .unwrap()
        .expect("visible entry should exist");

    assert_eq!(visible.current_row.version_id(), newer_id);
    assert_eq!(
        visible.version_id_for_tier(DurabilityTier::GlobalServer),
        Some(older_id),
        "older globally settled row should still be the global winner"
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
        .add_row_version_with_update(&mut io, row_id, "main", row)
        .unwrap()
        .version_id;

    let update = manager
        .patch_row_version_state_with_storage(
            &mut io,
            row_id,
            &BranchName::new("main"),
            version_id,
            None,
            Some(DurabilityTier::EdgeServer),
        )
        .expect("durability changes should surface as row updates for the visible winner");
    assert_eq!(update.row.version_id(), version_id);
    assert_eq!(update.row.confirmed_tier, Some(DurabilityTier::EdgeServer));

    manager.patch_row_version_state_with_storage(
        &mut io,
        row_id,
        &BranchName::new("main"),
        version_id,
        None,
        Some(DurabilityTier::Worker),
    );

    let visible = load_visible_row(&io, row_id, "main");
    assert_eq!(visible.confirmed_tier, Some(DurabilityTier::EdgeServer));
}

#[test]
fn storage_loads_row_metadata_and_branch_tips_without_object_manager_hydration() {
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

    let loaded = writer_storage
        .load_row_locator(row_id)
        .unwrap()
        .map(|locator| crate::storage::metadata_from_row_locator(&locator))
        .expect("row metadata should load from storage");

    assert_eq!(
        loaded.get(MetadataKey::Table.as_str()),
        Some(&"users".to_string())
    );
    assert_eq!(
        load_visible_row(&writer_storage, row_id, "main").version_id(),
        newer_id
    );
    let tips = writer_storage
        .scan_row_branch_tip_ids("users", "main", row_id)
        .unwrap();
    assert_eq!(tips.len(), 1);
    assert!(tips.contains(&newer_id));
}
