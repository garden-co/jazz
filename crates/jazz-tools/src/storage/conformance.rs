//! Storage conformance test suite for the row-region/raw-table storage model.

use std::collections::HashMap;

use std::ops::Bound;

use crate::catalogue::CatalogueEntry;
use crate::metadata::{MetadataKey, ObjectType, RowProvenance};
use crate::object::ObjectId;
use crate::query_manager::types::Value;
use crate::row_histories::{HistoryScan, RowState, StoredRowVersion, VisibleRowEntry};
use crate::storage::{IndexMutation, Storage};
use crate::sync_manager::DurabilityTier;

/// Factory type for persistence tests that reopen storage at a given path.
pub type PersistentStorageFactory = dyn Fn(&std::path::Path) -> Box<dyn Storage>;

fn make_row_version(
    row_id: ObjectId,
    branch: &str,
    updated_at: u64,
    value: &[u8],
) -> StoredRowVersion {
    StoredRowVersion::new(
        row_id,
        branch,
        Vec::new(),
        value.to_vec(),
        RowProvenance::for_insert(row_id.to_string(), updated_at),
        HashMap::new(),
        RowState::VisibleDirect,
        None,
    )
}

fn make_visible_entry(
    current_row: StoredRowVersion,
    history_rows: &[StoredRowVersion],
) -> VisibleRowEntry {
    VisibleRowEntry::rebuild(current_row, history_rows)
}

// ============================================================================
// Object metadata tests
// ============================================================================

pub fn test_object_create_and_load_metadata(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let object_id = ObjectId::new();
    let metadata = HashMap::from([
        ("owner".to_string(), "alice".to_string()),
        ("role".to_string(), "admin".to_string()),
    ]);

    storage.put_metadata(object_id, metadata.clone()).unwrap();

    assert_eq!(storage.load_metadata(object_id).unwrap().unwrap(), metadata);
}

pub fn test_object_load_nonexistent_returns_none(factory: &dyn Fn() -> Box<dyn Storage>) {
    let storage = factory();
    assert!(storage.load_metadata(ObjectId::new()).unwrap().is_none());
}

pub fn test_object_metadata_isolation(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    storage
        .put_metadata(
            alice,
            HashMap::from([("owner".to_string(), "alice".to_string())]),
        )
        .unwrap();
    storage
        .put_metadata(
            bob,
            HashMap::from([("owner".to_string(), "bob".to_string())]),
        )
        .unwrap();

    assert_eq!(
        storage.load_metadata(alice).unwrap().unwrap()["owner"],
        "alice"
    );
    assert_eq!(storage.load_metadata(bob).unwrap().unwrap()["owner"], "bob");
}

// ============================================================================
// Ordered raw-table tests
// ============================================================================

pub fn test_raw_table_round_trip(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    storage.raw_table_put("users", "alice", b"hello").unwrap();
    assert_eq!(
        storage.raw_table_get("users", "alice").unwrap(),
        Some(b"hello".to_vec())
    );

    storage.raw_table_delete("users", "alice").unwrap();
    assert_eq!(storage.raw_table_get("users", "alice").unwrap(), None);
}

pub fn test_raw_table_scan_prefix(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    storage.raw_table_put("users", "alice/1", b"a").unwrap();
    storage.raw_table_put("users", "alice/2", b"b").unwrap();
    storage.raw_table_put("users", "bob/1", b"c").unwrap();

    let rows = storage.raw_table_scan_prefix("users", "alice/").unwrap();
    assert_eq!(
        rows,
        vec![
            ("alice/1".to_string(), b"a".to_vec()),
            ("alice/2".to_string(), b"b".to_vec()),
        ]
    );
}

pub fn test_raw_table_scan_range(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    storage.raw_table_put("users", "01", b"a").unwrap();
    storage.raw_table_put("users", "02", b"b").unwrap();
    storage.raw_table_put("users", "03", b"c").unwrap();

    let rows = storage
        .raw_table_scan_range("users", Some("02"), Some("04"))
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("02".to_string(), b"b".to_vec()),
            ("03".to_string(), b"c".to_vec()),
        ]
    );
}

// ============================================================================
// Index tests
// ============================================================================

pub fn test_index_insert_and_exact_lookup(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_id = ObjectId::new();

    storage
        .index_insert("users", "age", "main", &Value::Integer(25), row_id)
        .unwrap();

    assert_eq!(
        storage.index_lookup("users", "age", "main", &Value::Integer(25)),
        vec![row_id]
    );
}

pub fn test_index_duplicate_values(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    storage
        .index_insert("users", "age", "main", &Value::Integer(25), alice)
        .unwrap();
    storage
        .index_insert("users", "age", "main", &Value::Integer(25), bob)
        .unwrap();

    let mut rows = storage.index_lookup("users", "age", "main", &Value::Integer(25));
    rows.sort();
    let mut expected = vec![alice, bob];
    expected.sort();
    assert_eq!(rows, expected);
}

pub fn test_index_remove(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    storage
        .index_insert("users", "age", "main", &Value::Integer(25), alice)
        .unwrap();
    storage
        .index_insert("users", "age", "main", &Value::Integer(25), bob)
        .unwrap();
    storage
        .index_remove("users", "age", "main", &Value::Integer(25), alice)
        .unwrap();

    assert_eq!(
        storage.index_lookup("users", "age", "main", &Value::Integer(25)),
        vec![bob]
    );
}

pub fn test_index_range(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row1 = ObjectId::new();
    let row2 = ObjectId::new();
    let row3 = ObjectId::new();

    storage
        .index_insert("users", "age", "main", &Value::Integer(20), row1)
        .unwrap();
    storage
        .index_insert("users", "age", "main", &Value::Integer(25), row2)
        .unwrap();
    storage
        .index_insert("users", "age", "main", &Value::Integer(30), row3)
        .unwrap();

    assert_eq!(
        storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(25)),
            Bound::Unbounded,
        ),
        vec![row2, row3]
    );
}

pub fn test_index_cross_branch_isolation(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let main_row = ObjectId::new();
    let draft_row = ObjectId::new();

    storage
        .index_insert("users", "age", "main", &Value::Integer(25), main_row)
        .unwrap();
    storage
        .index_insert("users", "age", "draft", &Value::Integer(25), draft_row)
        .unwrap();

    assert_eq!(
        storage.index_lookup("users", "age", "main", &Value::Integer(25)),
        vec![main_row]
    );
    assert_eq!(
        storage.index_lookup("users", "age", "draft", &Value::Integer(25)),
        vec![draft_row]
    );
}

// ============================================================================
// Row-region tests
// ============================================================================

pub fn test_row_region_round_trip(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_id = ObjectId::new();
    let version = make_row_version(row_id, "main", 10, b"alice");
    let version_id = version.version_id();

    storage
        .append_history_region_rows("users", std::slice::from_ref(&version))
        .unwrap();
    storage
        .upsert_visible_region_rows(
            "users",
            std::slice::from_ref(&make_visible_entry(
                version.clone(),
                std::slice::from_ref(&version),
            )),
        )
        .unwrap();

    assert_eq!(
        storage
            .load_visible_region_row("users", "main", row_id)
            .unwrap(),
        Some(version.clone())
    );
    assert_eq!(
        storage.scan_visible_region("users", "main").unwrap(),
        vec![version.clone()]
    );
    assert_eq!(
        storage
            .scan_history_region("users", "main", HistoryScan::Row { row_id })
            .unwrap(),
        vec![version]
    );
    assert_eq!(
        storage
            .load_visible_region_frontier("users", "main", row_id)
            .unwrap(),
        Some(vec![version_id])
    );
}

pub fn test_row_region_patch_state_monotonic(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_id = ObjectId::new();
    let version = make_row_version(row_id, "main", 10, b"alice");

    storage
        .append_history_region_rows("users", std::slice::from_ref(&version))
        .unwrap();
    storage
        .upsert_visible_region_rows(
            "users",
            std::slice::from_ref(&make_visible_entry(
                version.clone(),
                std::slice::from_ref(&version),
            )),
        )
        .unwrap();

    storage
        .patch_row_region_rows_by_batch(
            "users",
            version.batch_id,
            None,
            Some(DurabilityTier::EdgeServer),
        )
        .unwrap();
    storage
        .patch_row_region_rows_by_batch(
            "users",
            version.batch_id,
            None,
            Some(DurabilityTier::Worker),
        )
        .unwrap();

    let visible = storage
        .load_visible_region_row("users", "main", row_id)
        .unwrap();
    let history = storage
        .scan_history_region("users", "main", HistoryScan::Row { row_id })
        .unwrap();

    assert_eq!(
        visible.and_then(|row| row.confirmed_tier),
        Some(DurabilityTier::EdgeServer)
    );
    assert_eq!(history[0].confirmed_tier, Some(DurabilityTier::EdgeServer));
}

pub fn test_row_region_branch_isolation(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let main_row = make_row_version(ObjectId::new(), "main", 10, b"main");
    let draft_row = make_row_version(ObjectId::new(), "draft", 20, b"draft");

    storage
        .append_history_region_rows("users", &[main_row.clone(), draft_row.clone()])
        .unwrap();
    storage
        .upsert_visible_region_rows(
            "users",
            &[
                make_visible_entry(main_row.clone(), std::slice::from_ref(&main_row)),
                make_visible_entry(draft_row.clone(), std::slice::from_ref(&draft_row)),
            ],
        )
        .unwrap();

    assert_eq!(
        storage.scan_visible_region("users", "main").unwrap(),
        vec![main_row]
    );
    assert_eq!(
        storage.scan_visible_region("users", "draft").unwrap(),
        vec![draft_row]
    );
}

pub fn test_row_region_cross_branch_visible_heads(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_id = ObjectId::new();
    let main_row = make_row_version(row_id, "main", 10, b"main");
    let draft_row = make_row_version(row_id, "draft", 20, b"draft");

    storage
        .append_history_region_rows("users", &[main_row.clone(), draft_row.clone()])
        .unwrap();
    storage
        .upsert_visible_region_rows(
            "users",
            &[
                make_visible_entry(main_row.clone(), std::slice::from_ref(&main_row)),
                make_visible_entry(draft_row.clone(), std::slice::from_ref(&draft_row)),
            ],
        )
        .unwrap();

    assert_eq!(
        storage
            .scan_visible_region_row_versions("users", row_id)
            .unwrap(),
        vec![draft_row, main_row]
    );
}

pub fn test_apply_row_mutation_combines_row_and_index_effects(
    factory: &dyn Fn() -> Box<dyn Storage>,
) {
    let mut storage = factory();
    let row_id = ObjectId::new();
    let version = make_row_version(row_id, "main", 10, b"alice");
    let visible_entry = make_visible_entry(version.clone(), std::slice::from_ref(&version));
    let index_mutations = [IndexMutation::Insert {
        table: "users",
        column: "name",
        branch: "main",
        value: Value::Text("alice".to_string()),
        row_id,
    }];

    storage
        .apply_row_mutation(
            "users",
            std::slice::from_ref(&version),
            std::slice::from_ref(&visible_entry),
            &index_mutations,
        )
        .unwrap();

    assert_eq!(
        storage
            .load_visible_region_row("users", "main", row_id)
            .unwrap(),
        Some(version.clone())
    );
    assert_eq!(
        storage
            .load_history_row_version("users", row_id, version.version_id())
            .unwrap(),
        Some(version.clone())
    );
    assert_eq!(
        storage.index_lookup("users", "name", "main", &Value::Text("alice".to_string())),
        vec![row_id]
    );
}

// ============================================================================
// Catalogue tests
// ============================================================================

pub fn test_catalogue_entry_round_trip(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let object_id = ObjectId::new();
    let entry = CatalogueEntry {
        object_id,
        metadata: HashMap::from([
            (
                MetadataKey::Type.to_string(),
                ObjectType::CatalogueSchema.to_string(),
            ),
            ("schema_hash".to_string(), "abc123".to_string()),
        ]),
        content: br#"{"tables":["users"]}"#.to_vec(),
    };

    storage.upsert_catalogue_entry(&entry).unwrap();
    assert_eq!(
        storage.load_catalogue_entry(object_id).unwrap(),
        Some(entry)
    );
}

pub fn test_catalogue_entry_scan_returns_sorted_entries(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let low_id = ObjectId::from_uuid(uuid::Uuid::nil());
    let high_id = ObjectId::new();

    storage
        .upsert_catalogue_entry(&CatalogueEntry {
            object_id: high_id,
            metadata: HashMap::from([(
                MetadataKey::Type.to_string(),
                ObjectType::CatalogueLens.to_string(),
            )]),
            content: b"lens".to_vec(),
        })
        .unwrap();
    storage
        .upsert_catalogue_entry(&CatalogueEntry {
            object_id: low_id,
            metadata: HashMap::from([(
                MetadataKey::Type.to_string(),
                ObjectType::CatalogueSchema.to_string(),
            )]),
            content: b"schema".to_vec(),
        })
        .unwrap();

    let scanned = storage.scan_catalogue_entries().unwrap();
    assert_eq!(scanned.len(), 2);
    assert_eq!(scanned[0].object_id, low_id);
    assert_eq!(scanned[1].object_id, high_id);
}

pub fn test_catalogue_entry_upsert_replaces_existing(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let object_id = ObjectId::new();
    let first = CatalogueEntry {
        object_id,
        metadata: HashMap::from([(
            MetadataKey::Type.to_string(),
            ObjectType::CataloguePermissionsHead.to_string(),
        )]),
        content: b"v1".to_vec(),
    };
    let second = CatalogueEntry {
        object_id,
        metadata: HashMap::from([
            (
                MetadataKey::Type.to_string(),
                ObjectType::CataloguePermissionsHead.to_string(),
            ),
            ("note".to_string(), "updated".to_string()),
        ]),
        content: b"v2".to_vec(),
    };

    storage.upsert_catalogue_entry(&first).unwrap();
    storage.upsert_catalogue_entry(&second).unwrap();

    assert_eq!(
        storage.load_catalogue_entry(object_id).unwrap(),
        Some(second)
    );
}

pub fn test_catalogue_entry_nonexistent_returns_none(factory: &dyn Fn() -> Box<dyn Storage>) {
    let storage = factory();
    assert!(
        storage
            .load_catalogue_entry(ObjectId::new())
            .unwrap()
            .is_none()
    );
}

// ============================================================================
// Persistence tests
// ============================================================================

pub fn test_persistence_survives_close_reopen(factory: &PersistentStorageFactory) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path();

    let object_id = ObjectId::new();
    let row_id = ObjectId::new();
    let version = make_row_version(row_id, "main", 10, b"alice");

    {
        let mut storage = factory(path);
        storage
            .put_metadata(
                object_id,
                HashMap::from([("owner".to_string(), "alice".to_string())]),
            )
            .unwrap();
        storage.raw_table_put("users", "alice", b"hello").unwrap();
        storage
            .append_history_region_rows("users", std::slice::from_ref(&version))
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                std::slice::from_ref(&make_visible_entry(
                    version.clone(),
                    std::slice::from_ref(&version),
                )),
            )
            .unwrap();
        storage
            .index_insert(
                "users",
                "name",
                "main",
                &Value::Text("alice".to_string()),
                row_id,
            )
            .unwrap();
        storage.flush();
        storage.close().unwrap();
    }

    {
        let storage = factory(path);
        assert_eq!(
            storage.load_metadata(object_id).unwrap().unwrap()["owner"],
            "alice"
        );
        assert_eq!(
            storage.raw_table_get("users", "alice").unwrap(),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            storage
                .load_visible_region_row("users", "main", row_id)
                .unwrap(),
            Some(version.clone())
        );
        assert_eq!(
            storage.index_lookup("users", "name", "main", &Value::Text("alice".to_string())),
            vec![row_id]
        );
    }
}

pub fn test_close_releases_resources_for_reopen(factory: &PersistentStorageFactory) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path();

    factory(path).close().unwrap();
    factory(path).close().unwrap();
}

// ============================================================================
// Multi-actor test
// ============================================================================

pub fn test_alice_bob_branch_isolation(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let main_row = make_row_version(ObjectId::new(), "main", 10, b"alice");
    let draft_row = make_row_version(ObjectId::new(), "draft", 20, b"bob");

    storage
        .append_history_region_rows("users", &[main_row.clone(), draft_row.clone()])
        .unwrap();
    storage
        .upsert_visible_region_rows(
            "users",
            &[
                make_visible_entry(main_row.clone(), std::slice::from_ref(&main_row)),
                make_visible_entry(draft_row.clone(), std::slice::from_ref(&draft_row)),
            ],
        )
        .unwrap();
    storage
        .index_insert(
            "users",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            main_row.row_id,
        )
        .unwrap();
    storage
        .index_insert(
            "users",
            "name",
            "draft",
            &Value::Text("bob".to_string()),
            draft_row.row_id,
        )
        .unwrap();

    assert_eq!(
        storage.scan_visible_region("users", "main").unwrap(),
        vec![main_row.clone()]
    );
    assert_eq!(
        storage.scan_visible_region("users", "draft").unwrap(),
        vec![draft_row.clone()]
    );
    assert_eq!(
        storage.index_lookup("users", "name", "main", &Value::Text("alice".to_string())),
        vec![main_row.row_id]
    );
    assert_eq!(
        storage.index_lookup("users", "name", "draft", &Value::Text("bob".to_string())),
        vec![draft_row.row_id]
    );
}

// ============================================================================
// Macros
// ============================================================================

#[macro_export]
macro_rules! storage_conformance_tests {
    ($prefix:ident, $factory:expr) => {
        mod $prefix {
            use super::*;
            use $crate::storage::conformance;

            #[test]
            fn object_create_and_load_metadata() {
                conformance::test_object_create_and_load_metadata(&$factory);
            }

            #[test]
            fn object_load_nonexistent_returns_none() {
                conformance::test_object_load_nonexistent_returns_none(&$factory);
            }

            #[test]
            fn object_metadata_isolation() {
                conformance::test_object_metadata_isolation(&$factory);
            }

            #[test]
            fn raw_table_round_trip() {
                conformance::test_raw_table_round_trip(&$factory);
            }

            #[test]
            fn raw_table_scan_prefix() {
                conformance::test_raw_table_scan_prefix(&$factory);
            }

            #[test]
            fn raw_table_scan_range() {
                conformance::test_raw_table_scan_range(&$factory);
            }

            #[test]
            fn index_insert_and_exact_lookup() {
                conformance::test_index_insert_and_exact_lookup(&$factory);
            }

            #[test]
            fn index_duplicate_values() {
                conformance::test_index_duplicate_values(&$factory);
            }

            #[test]
            fn index_remove() {
                conformance::test_index_remove(&$factory);
            }

            #[test]
            fn index_range() {
                conformance::test_index_range(&$factory);
            }

            #[test]
            fn index_cross_branch_isolation() {
                conformance::test_index_cross_branch_isolation(&$factory);
            }

            #[test]
            fn row_region_round_trip() {
                conformance::test_row_region_round_trip(&$factory);
            }

            #[test]
            fn row_region_patch_state_monotonic() {
                conformance::test_row_region_patch_state_monotonic(&$factory);
            }

            #[test]
            fn row_region_branch_isolation() {
                conformance::test_row_region_branch_isolation(&$factory);
            }

            #[test]
            fn row_region_cross_branch_visible_heads() {
                conformance::test_row_region_cross_branch_visible_heads(&$factory);
            }

            #[test]
            fn apply_row_mutation_combines_row_and_index_effects() {
                conformance::test_apply_row_mutation_combines_row_and_index_effects(&$factory);
            }

            #[test]
            fn catalogue_entry_round_trip() {
                conformance::test_catalogue_entry_round_trip(&$factory);
            }

            #[test]
            fn catalogue_entry_scan_returns_sorted_entries() {
                conformance::test_catalogue_entry_scan_returns_sorted_entries(&$factory);
            }

            #[test]
            fn catalogue_entry_upsert_replaces_existing() {
                conformance::test_catalogue_entry_upsert_replaces_existing(&$factory);
            }

            #[test]
            fn catalogue_entry_nonexistent_returns_none() {
                conformance::test_catalogue_entry_nonexistent_returns_none(&$factory);
            }

            #[test]
            fn alice_bob_branch_isolation() {
                conformance::test_alice_bob_branch_isolation(&$factory);
            }
        }
    };
}

#[macro_export]
macro_rules! storage_conformance_tests_persistent {
    ($prefix:ident, $factory:expr, $reopen_factory:expr) => {
        $crate::storage_conformance_tests!($prefix, $factory);

        mod paste_persistent {
            use super::*;
            use $crate::storage::conformance;

            #[test]
            fn persistence_survives_close_reopen() {
                conformance::test_persistence_survives_close_reopen(&$reopen_factory);
            }

            #[test]
            fn close_releases_resources_for_reopen() {
                conformance::test_close_releases_resources_for_reopen(&$reopen_factory);
            }
        }
    };
}
