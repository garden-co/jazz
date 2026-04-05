//! Storage conformance test suite.
//!
//! Shared tests that any `Storage` backend can plug into with one macro invocation.
//! Each `test_*` function exercises a single contract of the `Storage` trait.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{BatchId, QueryBranchRef, SchemaHash, Value};
use crate::storage::{CatalogueLensSeen, CatalogueManifestOp, LoadedBranch, Storage, StorageError};
use crate::sync_manager::DurabilityTier;

/// Factory type for persistence tests that reopen storage at a given path.
pub type PersistentStorageFactory = dyn Fn(&std::path::Path) -> Box<dyn Storage>;

/// Build a commit with the given content, author, and parents.
pub fn make_commit(content: &[u8], author: ObjectId, parents: &[CommitId]) -> Commit {
    Commit {
        parents: parents.iter().copied().collect(),
        content: content.to_vec(),
        timestamp: 1_700_000_000_000_000,
        author: author.to_string(),
        metadata: None,
        stored_state: Default::default(),
        ack_state: Default::default(),
    }
}

fn test_branch_name(user_branch: &str) -> BranchName {
    branch_ref(user_branch).branch_name()
}

fn branch_ref(branch: &str) -> QueryBranchRef {
    let branch_name = BranchName::new(branch);
    if crate::query_manager::types::ComposedBranchName::parse(&branch_name).is_some() {
        return QueryBranchRef::from_branch_name(branch_name);
    }

    let prefix = crate::query_manager::types::BranchPrefixName::new(
        "dev",
        SchemaHash::from_bytes([7; 32]),
        branch,
    );
    let batch_id = BatchId::from_uuid(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        branch.as_bytes(),
    ));
    QueryBranchRef::from_prefix_and_batch(&prefix, batch_id)
}

trait IntoCompatBranchRef {
    fn into_compat_branch_ref(self) -> QueryBranchRef;
}

impl IntoCompatBranchRef for &str {
    fn into_compat_branch_ref(self) -> QueryBranchRef {
        branch_ref(self)
    }
}

impl IntoCompatBranchRef for &BranchName {
    fn into_compat_branch_ref(self) -> QueryBranchRef {
        branch_ref(self.as_str())
    }
}

impl IntoCompatBranchRef for BranchName {
    fn into_compat_branch_ref(self) -> QueryBranchRef {
        branch_ref(self.as_str())
    }
}

impl IntoCompatBranchRef for &QueryBranchRef {
    fn into_compat_branch_ref(self) -> QueryBranchRef {
        *self
    }
}

impl IntoCompatBranchRef for QueryBranchRef {
    fn into_compat_branch_ref(self) -> QueryBranchRef {
        self
    }
}

trait StorageTestCompat: Storage {
    fn compat_load_branch<B: IntoCompatBranchRef>(
        &self,
        object_id: ObjectId,
        branch: B,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        let branch = branch.into_compat_branch_ref();
        Storage::load_branch(self, object_id, &branch)
    }

    fn compat_append_commit<B: IntoCompatBranchRef>(
        &mut self,
        object_id: ObjectId,
        branch: B,
        commit: Commit,
    ) -> Result<(), StorageError> {
        let branch = branch.into_compat_branch_ref();
        Storage::append_commit(self, object_id, &branch, &commit, None)
    }

    fn compat_delete_commit<B: IntoCompatBranchRef>(
        &mut self,
        object_id: ObjectId,
        branch: B,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        let branch = branch.into_compat_branch_ref();
        let Some(loaded) = Storage::load_branch(self, object_id, &branch)? else {
            return Ok(());
        };

        let commits: Vec<Commit> = loaded
            .commits
            .into_iter()
            .filter(|commit| commit.id() != commit_id)
            .collect();
        let tails = loaded
            .tails
            .into_iter()
            .filter(|tail_id| *tail_id != commit_id)
            .collect();
        Storage::replace_branch(self, object_id, &branch, commits, tails)
    }

    fn compat_set_branch_tails<B: IntoCompatBranchRef>(
        &mut self,
        object_id: ObjectId,
        branch: B,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        let branch = branch.into_compat_branch_ref();
        let loaded = Storage::load_branch(self, object_id, &branch)?.unwrap_or_default();
        Storage::replace_branch(
            self,
            object_id,
            &branch,
            loaded.commits,
            tails.unwrap_or_default().into_iter().collect(),
        )
    }

    fn compat_index_insert<B: IntoCompatBranchRef>(
        &mut self,
        table: &str,
        column: &str,
        branch: B,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let branch = branch.into_compat_branch_ref();
        Storage::index_insert(self, table, column, &branch, value, row_id)
    }

    fn compat_index_remove<B: IntoCompatBranchRef>(
        &mut self,
        table: &str,
        column: &str,
        branch: B,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let branch = branch.into_compat_branch_ref();
        Storage::index_remove(self, table, column, &branch, value, row_id)
    }

    fn compat_index_lookup<B: IntoCompatBranchRef>(
        &self,
        table: &str,
        column: &str,
        branch: B,
        value: &Value,
    ) -> Vec<ObjectId> {
        let branch = branch.into_compat_branch_ref();
        Storage::index_lookup(self, table, column, &branch, value)
    }

    fn compat_index_range<B: IntoCompatBranchRef>(
        &self,
        table: &str,
        column: &str,
        branch: B,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let branch = branch.into_compat_branch_ref();
        Storage::index_range(self, table, column, &branch, start, end)
    }

    fn compat_index_scan_all<B: IntoCompatBranchRef>(
        &self,
        table: &str,
        column: &str,
        branch: B,
    ) -> Vec<ObjectId> {
        let branch = branch.into_compat_branch_ref();
        Storage::index_scan_all(self, table, column, &branch)
    }
}

impl<T: Storage + ?Sized> StorageTestCompat for T {}

// ============================================================================
// Object storage tests
// ============================================================================

pub fn test_object_create_and_load_metadata(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();

    let mut meta = HashMap::new();
    meta.insert("owner".to_string(), "alice".to_string());
    meta.insert("role".to_string(), "admin".to_string());

    storage.create_object(alice, meta.clone()).unwrap();

    let loaded = storage.load_object_metadata(alice).unwrap().unwrap();
    assert_eq!(loaded, meta);
}

pub fn test_object_load_nonexistent_returns_none(factory: &dyn Fn() -> Box<dyn Storage>) {
    let storage = factory();
    let unknown = ObjectId::new();

    let result = storage.load_object_metadata(unknown).unwrap();
    assert!(result.is_none());
}

pub fn test_object_metadata_isolation(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    let mut alice_meta = HashMap::new();
    alice_meta.insert("owner".to_string(), "alice".to_string());

    let mut bob_meta = HashMap::new();
    bob_meta.insert("owner".to_string(), "bob".to_string());

    storage.create_object(alice, alice_meta.clone()).unwrap();
    storage.create_object(bob, bob_meta.clone()).unwrap();

    let loaded_alice = storage.load_object_metadata(alice).unwrap().unwrap();
    let loaded_bob = storage.load_object_metadata(bob).unwrap().unwrap();

    assert_eq!(loaded_alice, alice_meta);
    assert_eq!(loaded_bob, bob_meta);
    assert_ne!(loaded_alice, loaded_bob);
}

// ============================================================================
// Branch & commit tests
// ============================================================================

pub fn test_branch_load_nonexistent_returns_none(factory: &dyn Fn() -> Box<dyn Storage>) {
    let storage = factory();
    let obj = ObjectId::new();
    let branch = test_branch_name("main");

    let result = storage.compat_load_branch(obj, branch).unwrap();
    assert!(result.is_none());
}

pub fn test_commit_append_and_load(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let branch = test_branch_name("main");

    let mut meta = HashMap::new();
    meta.insert("owner".to_string(), "alice".to_string());
    storage.create_object(alice, meta).unwrap();

    let c1 = make_commit(b"alice's first edit", alice, &[]);
    let c1_id = c1.id();
    storage.compat_append_commit(alice, branch, c1).unwrap();

    let loaded = storage.compat_load_branch(alice, branch).unwrap().unwrap();
    assert_eq!(loaded.commits.len(), 1);
    assert_eq!(loaded.commits[0].content, b"alice's first edit");
    assert_eq!(loaded.commits[0].id(), c1_id);
    // Single root commit => tails should contain that commit
    assert!(loaded.tails.contains(&c1_id));
}

pub fn test_commit_append_chain(factory: &dyn Fn() -> Box<dyn Storage>) {
    //  c1 ("draft v1")  -->  c2 ("draft v2")
    //  (root)                (parent: c1)
    //
    //  After appending both, tails should point to c2 (the tip),
    //  and both commits should be loadable.

    let mut storage = factory();
    let alice = ObjectId::new();
    let branch = test_branch_name("main");

    let mut meta = HashMap::new();
    meta.insert("owner".to_string(), "alice".to_string());
    storage.create_object(alice, meta).unwrap();

    let c1 = make_commit(b"draft v1", alice, &[]);
    let c1_id = c1.id();
    storage.compat_append_commit(alice, branch, c1).unwrap();

    let c2 = make_commit(b"draft v2", alice, &[c1_id]);
    let c2_id = c2.id();
    storage.compat_append_commit(alice, branch, c2).unwrap();

    let loaded = storage.compat_load_branch(alice, branch).unwrap().unwrap();
    assert_eq!(loaded.commits.len(), 2);

    let ids: HashSet<CommitId> = loaded.commits.iter().map(|c| c.id()).collect();
    assert!(ids.contains(&c1_id));
    assert!(ids.contains(&c2_id));

    // Tails should reflect the tip (c2)
    assert!(loaded.tails.contains(&c2_id));
}

pub fn test_commit_delete(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let branch = test_branch_name("main");

    let mut meta = HashMap::new();
    meta.insert("owner".to_string(), "alice".to_string());
    storage.create_object(alice, meta).unwrap();

    let c1 = make_commit(b"first", alice, &[]);
    let c1_id = c1.id();
    storage.compat_append_commit(alice, branch, c1).unwrap();

    let c2 = make_commit(b"second", alice, &[c1_id]);
    let c2_id = c2.id();
    storage.compat_append_commit(alice, branch, c2).unwrap();

    storage.compat_delete_commit(alice, branch, c1_id).unwrap();

    let loaded = storage.compat_load_branch(alice, branch).unwrap().unwrap();
    assert_eq!(loaded.commits.len(), 1);
    assert_eq!(loaded.commits[0].id(), c2_id);
}

pub fn test_branch_tails_set_and_clear(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let branch = test_branch_name("main");

    let mut meta = HashMap::new();
    meta.insert("owner".to_string(), "alice".to_string());
    storage.create_object(alice, meta).unwrap();

    let c1 = make_commit(b"root", alice, &[]);
    let c1_id = c1.id();
    storage.compat_append_commit(alice, branch, c1).unwrap();

    // Set explicit tails
    let mut explicit_tails = HashSet::new();
    explicit_tails.insert(c1_id);
    storage
        .compat_set_branch_tails(alice, branch, Some(explicit_tails.clone()))
        .unwrap();

    let loaded = storage.compat_load_branch(alice, branch).unwrap().unwrap();
    assert_eq!(
        loaded.tails.into_iter().collect::<HashSet<_>>(),
        explicit_tails
    );

    // Clear tails
    storage
        .compat_set_branch_tails(alice, branch, None)
        .unwrap();

    let loaded = storage.compat_load_branch(alice, branch).unwrap().unwrap();
    assert!(loaded.tails.is_empty());
}

pub fn test_multiple_branches_independent(factory: &dyn Fn() -> Box<dyn Storage>) {
    //  Object: tasks
    //
    //  "main" branch (alice):   c_main ("publish v1")
    //  "draft" branch (bob):    c_draft ("wip notes")
    //
    //  Each branch loads independently without cross-contamination.

    let mut storage = factory();
    let tasks = ObjectId::new();
    let main_branch = test_branch_name("main");
    let draft_branch = test_branch_name("draft");

    let mut meta = HashMap::new();
    meta.insert("type".to_string(), "tasks".to_string());
    storage.create_object(tasks, meta).unwrap();

    let alice = ObjectId::new();
    let bob = ObjectId::new();

    let c_main = make_commit(b"publish v1", alice, &[]);
    let c_main_id = c_main.id();
    storage
        .compat_append_commit(tasks, main_branch, c_main)
        .unwrap();

    let c_draft = make_commit(b"wip notes", bob, &[]);
    let c_draft_id = c_draft.id();
    storage
        .compat_append_commit(tasks, draft_branch, c_draft)
        .unwrap();

    let loaded_main = storage
        .compat_load_branch(tasks, main_branch)
        .unwrap()
        .unwrap();
    assert_eq!(loaded_main.commits.len(), 1);
    assert_eq!(loaded_main.commits[0].id(), c_main_id);

    let loaded_draft = storage
        .compat_load_branch(tasks, draft_branch)
        .unwrap()
        .unwrap();
    assert_eq!(loaded_draft.commits.len(), 1);
    assert_eq!(loaded_draft.commits[0].id(), c_draft_id);
}

// ============================================================================
// Index operation tests
// ============================================================================

pub fn test_index_insert_and_exact_lookup(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_alice = ObjectId::new();

    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            row_alice,
        )
        .unwrap();

    let results =
        storage.compat_index_lookup("users", "name", "main", &Value::Text("alice".to_string()));
    assert_eq!(results, vec![row_alice]);

    let empty =
        storage.compat_index_lookup("users", "name", "main", &Value::Text("bob".to_string()));
    assert!(empty.is_empty());
}

pub fn test_index_duplicate_values(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row1 = ObjectId::new();
    let row2 = ObjectId::new();

    storage
        .compat_index_insert("users", "age", "main", &Value::Integer(30), row1)
        .unwrap();
    storage
        .compat_index_insert("users", "age", "main", &Value::Integer(30), row2)
        .unwrap();

    let mut results = storage.compat_index_lookup("users", "age", "main", &Value::Integer(30));
    results.sort();
    let mut expected = vec![row1, row2];
    expected.sort();
    assert_eq!(results, expected);
}

pub fn test_index_remove(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row1 = ObjectId::new();
    let row2 = ObjectId::new();

    storage
        .compat_index_insert("users", "age", "main", &Value::Integer(25), row1)
        .unwrap();
    storage
        .compat_index_insert("users", "age", "main", &Value::Integer(25), row2)
        .unwrap();

    storage
        .compat_index_remove("users", "age", "main", &Value::Integer(25), row1)
        .unwrap();

    let results = storage.compat_index_lookup("users", "age", "main", &Value::Integer(25));
    assert_eq!(results, vec![row2]);
}

pub fn test_index_range_inclusive_exclusive(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_20 = ObjectId::new();
    let row_25 = ObjectId::new();
    let row_28 = ObjectId::new();
    let row_30 = ObjectId::new();
    let row_35 = ObjectId::new();

    for (age, row) in [
        (20, row_20),
        (25, row_25),
        (28, row_28),
        (30, row_30),
        (35, row_35),
    ] {
        storage
            .compat_index_insert("users", "age", "main", &Value::Integer(age), row)
            .unwrap();
    }

    // [25, 30) — inclusive start, exclusive end
    let mut results = storage.compat_index_range(
        "users",
        "age",
        "main",
        Bound::Included(&Value::Integer(25)),
        Bound::Excluded(&Value::Integer(30)),
    );
    results.sort();
    let mut expected = vec![row_25, row_28];
    expected.sort();
    assert_eq!(results, expected);
}

pub fn test_index_range_unbounded_start(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_20 = ObjectId::new();
    let row_25 = ObjectId::new();
    let row_26 = ObjectId::new();
    let row_30 = ObjectId::new();

    for (age, row) in [(20, row_20), (25, row_25), (26, row_26), (30, row_30)] {
        storage
            .compat_index_insert("users", "age", "main", &Value::Integer(age), row)
            .unwrap();
    }

    // (Unbounded, Excluded(26))
    let mut results = storage.compat_index_range(
        "users",
        "age",
        "main",
        Bound::Unbounded,
        Bound::Excluded(&Value::Integer(26)),
    );
    results.sort();
    let mut expected = vec![row_20, row_25];
    expected.sort();
    assert_eq!(results, expected);
}

pub fn test_index_range_unbounded_end(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_20 = ObjectId::new();
    let row_25 = ObjectId::new();
    let row_30 = ObjectId::new();

    for (age, row) in [(20, row_20), (25, row_25), (30, row_30)] {
        storage
            .compat_index_insert("users", "age", "main", &Value::Integer(age), row)
            .unwrap();
    }

    // [25, Unbounded)
    let mut results = storage.compat_index_range(
        "users",
        "age",
        "main",
        Bound::Included(&Value::Integer(25)),
        Bound::Unbounded,
    );
    results.sort();
    let mut expected = vec![row_25, row_30];
    expected.sort();
    assert_eq!(results, expected);
}

pub fn test_index_scan_all(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row1 = ObjectId::new();
    let row2 = ObjectId::new();
    let row3 = ObjectId::new();

    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            row1,
        )
        .unwrap();
    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("bob".to_string()),
            row2,
        )
        .unwrap();
    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("carol".to_string()),
            row3,
        )
        .unwrap();

    let mut results = storage.compat_index_scan_all("users", "name", "main");
    results.sort();
    let mut expected = vec![row1, row2, row3];
    expected.sort();
    assert_eq!(results, expected);
}

pub fn test_index_cross_table_isolation(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let user_row = ObjectId::new();
    let post_row = ObjectId::new();

    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            user_row,
        )
        .unwrap();
    storage
        .compat_index_insert(
            "posts",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            post_row,
        )
        .unwrap();

    let user_results =
        storage.compat_index_lookup("users", "name", "main", &Value::Text("alice".to_string()));
    assert_eq!(user_results, vec![user_row]);

    let post_results =
        storage.compat_index_lookup("posts", "name", "main", &Value::Text("alice".to_string()));
    assert_eq!(post_results, vec![post_row]);
}

pub fn test_index_cross_branch_isolation(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let main_row = ObjectId::new();
    let draft_row = ObjectId::new();

    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            main_row,
        )
        .unwrap();
    storage
        .compat_index_insert(
            "users",
            "name",
            "draft",
            &Value::Text("alice".to_string()),
            draft_row,
        )
        .unwrap();

    let main_results =
        storage.compat_index_lookup("users", "name", "main", &Value::Text("alice".to_string()));
    assert_eq!(main_results, vec![main_row]);

    let draft_results =
        storage.compat_index_lookup("users", "name", "draft", &Value::Text("alice".to_string()));
    assert_eq!(draft_results, vec![draft_row]);
}

pub fn test_index_value_types(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let row_int = ObjectId::new();
    let row_text = ObjectId::new();
    let row_uuid = ObjectId::new();
    let uuid_val = ObjectId::new();

    storage
        .compat_index_insert("users", "age", "main", &Value::Integer(42), row_int)
        .unwrap();
    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            row_text,
        )
        .unwrap();
    storage
        .compat_index_insert("users", "ref_id", "main", &Value::Uuid(uuid_val), row_uuid)
        .unwrap();

    let int_results = storage.compat_index_lookup("users", "age", "main", &Value::Integer(42));
    assert_eq!(int_results, vec![row_int]);

    let text_results =
        storage.compat_index_lookup("users", "name", "main", &Value::Text("alice".to_string()));
    assert_eq!(text_results, vec![row_text]);

    let uuid_results =
        storage.compat_index_lookup("users", "ref_id", "main", &Value::Uuid(uuid_val));
    assert_eq!(uuid_results, vec![row_uuid]);
}

// ============================================================================
// Ack tier test
// ============================================================================

pub fn test_store_and_load_ack_tier(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let alice = ObjectId::new();
    let branch = test_branch_name("main");

    let mut meta = HashMap::new();
    meta.insert("owner".to_string(), "alice".to_string());
    storage.create_object(alice, meta).unwrap();

    let c1 = make_commit(b"synced edit", alice, &[]);
    let c1_id = c1.id();
    storage.compat_append_commit(alice, branch, c1).unwrap();

    storage
        .store_ack_tier(c1_id, DurabilityTier::Worker)
        .unwrap();

    let loaded = storage.compat_load_branch(alice, branch).unwrap().unwrap();
    assert_eq!(loaded.commits.len(), 1);
    assert!(
        loaded.commits[0]
            .ack_state
            .confirmed_tiers
            .contains(&DurabilityTier::Worker)
    );
}

// ============================================================================
// Catalogue manifest tests
// ============================================================================

pub fn test_catalogue_manifest_schema_seen(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let app_id = ObjectId::new();
    let obj_id = ObjectId::new();
    let schema_hash = SchemaHash::from_bytes([0x11; 32]);

    storage
        .append_catalogue_manifest_op(
            app_id,
            CatalogueManifestOp::SchemaSeen {
                object_id: obj_id,
                schema_hash,
            },
        )
        .unwrap();

    let manifest = storage.load_catalogue_manifest(app_id).unwrap().unwrap();
    assert_eq!(manifest.schema_seen.get(&obj_id), Some(&schema_hash));
}

pub fn test_catalogue_manifest_lens_seen(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let app_id = ObjectId::new();
    let obj_id = ObjectId::new();
    let source = SchemaHash::from_bytes([0x22; 32]);
    let target = SchemaHash::from_bytes([0x33; 32]);

    storage
        .append_catalogue_manifest_op(
            app_id,
            CatalogueManifestOp::LensSeen {
                object_id: obj_id,
                source_hash: source,
                target_hash: target,
            },
        )
        .unwrap();

    let manifest = storage.load_catalogue_manifest(app_id).unwrap().unwrap();
    let lens = manifest.lens_seen.get(&obj_id).unwrap();
    assert_eq!(
        *lens,
        CatalogueLensSeen {
            source_hash: source,
            target_hash: target,
        }
    );
}

pub fn test_catalogue_manifest_idempotent(factory: &dyn Fn() -> Box<dyn Storage>) {
    let mut storage = factory();
    let app_id = ObjectId::new();
    let obj_id = ObjectId::new();
    let schema_hash = SchemaHash::from_bytes([0x44; 32]);

    let op = CatalogueManifestOp::SchemaSeen {
        object_id: obj_id,
        schema_hash,
    };

    storage
        .append_catalogue_manifest_op(app_id, op.clone())
        .unwrap();
    storage.append_catalogue_manifest_op(app_id, op).unwrap();

    let manifest = storage.load_catalogue_manifest(app_id).unwrap().unwrap();
    assert_eq!(manifest.schema_seen.len(), 1);
    assert_eq!(manifest.schema_seen.get(&obj_id), Some(&schema_hash));
}

pub fn test_catalogue_manifest_nonexistent_returns_none(factory: &dyn Fn() -> Box<dyn Storage>) {
    let storage = factory();
    let unknown_app = ObjectId::new();

    let result = storage.load_catalogue_manifest(unknown_app).unwrap();
    assert!(result.is_none());
}

// ============================================================================
// Persistence tests
// ============================================================================

pub fn test_persistence_survives_close_reopen(factory: &PersistentStorageFactory) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path();

    let alice = ObjectId::new();
    let branch = test_branch_name("main");
    let row_id = ObjectId::new();

    // Write phase
    {
        let mut storage = factory(path);

        let mut meta = HashMap::new();
        meta.insert("owner".to_string(), "alice".to_string());
        storage.create_object(alice, meta).unwrap();

        let c1 = make_commit(b"persistent edit", alice, &[]);
        storage.compat_append_commit(alice, branch, c1).unwrap();

        storage
            .compat_index_insert(
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

    // Reopen and verify
    {
        let storage = factory(path);

        let meta = storage.load_object_metadata(alice).unwrap().unwrap();
        assert_eq!(meta.get("owner").unwrap(), "alice");

        let loaded = storage.compat_load_branch(alice, branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(loaded.commits[0].content, b"persistent edit");

        let results =
            storage.compat_index_lookup("users", "name", "main", &Value::Text("alice".to_string()));
        assert_eq!(results, vec![row_id]);
    }
}

pub fn test_close_releases_resources_for_reopen(factory: &PersistentStorageFactory) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path();

    {
        let storage = factory(path);
        storage.close().unwrap();
    }

    // Reopening at the same path should succeed
    {
        let storage = factory(path);
        storage.close().unwrap();
    }
}

// ============================================================================
// Multi-actor test
// ============================================================================

pub fn test_alice_bob_concurrent_branches(factory: &dyn Fn() -> Box<dyn Storage>) {
    //  Object: tasks
    //
    //  alice -> "main" branch:  commit "alice publishes"
    //           index: users/name/main = "alice" -> row_alice
    //
    //  bob   -> "draft" branch: commit "bob drafts"
    //           index: users/name/draft = "bob" -> row_bob
    //
    //  Verify: branches and indices are fully isolated.

    let mut storage = factory();
    let tasks = ObjectId::new();

    let mut meta = HashMap::new();
    meta.insert("type".to_string(), "tasks".to_string());
    storage.create_object(tasks, meta).unwrap();

    let alice = ObjectId::new();
    let bob = ObjectId::new();
    let main_branch = test_branch_name("main");
    let draft_branch = test_branch_name("draft");

    // Alice writes to main
    let c_alice = make_commit(b"alice publishes", alice, &[]);
    let c_alice_id = c_alice.id();
    storage
        .compat_append_commit(tasks, main_branch, c_alice)
        .unwrap();

    let row_alice = ObjectId::new();
    storage
        .compat_index_insert(
            "users",
            "name",
            "main",
            &Value::Text("alice".to_string()),
            row_alice,
        )
        .unwrap();

    // Bob writes to draft
    let c_bob = make_commit(b"bob drafts", bob, &[]);
    let c_bob_id = c_bob.id();
    storage
        .compat_append_commit(tasks, draft_branch, c_bob)
        .unwrap();

    let row_bob = ObjectId::new();
    storage
        .compat_index_insert(
            "users",
            "name",
            "draft",
            &Value::Text("bob".to_string()),
            row_bob,
        )
        .unwrap();

    // Verify branch isolation
    let loaded_main = storage
        .compat_load_branch(tasks, main_branch)
        .unwrap()
        .unwrap();
    assert_eq!(loaded_main.commits.len(), 1);
    assert_eq!(loaded_main.commits[0].id(), c_alice_id);

    let loaded_draft = storage
        .compat_load_branch(tasks, draft_branch)
        .unwrap()
        .unwrap();
    assert_eq!(loaded_draft.commits.len(), 1);
    assert_eq!(loaded_draft.commits[0].id(), c_bob_id);

    // Verify index isolation
    let main_results =
        storage.compat_index_lookup("users", "name", "main", &Value::Text("alice".to_string()));
    assert_eq!(main_results, vec![row_alice]);
    let main_bob =
        storage.compat_index_lookup("users", "name", "main", &Value::Text("bob".to_string()));
    assert!(main_bob.is_empty());

    let draft_results =
        storage.compat_index_lookup("users", "name", "draft", &Value::Text("bob".to_string()));
    assert_eq!(draft_results, vec![row_bob]);
    let draft_alice =
        storage.compat_index_lookup("users", "name", "draft", &Value::Text("alice".to_string()));
    assert!(draft_alice.is_empty());
}

pub fn test_index_composed_prefix_isolation_with_same_batch_id(
    factory: &dyn Fn() -> Box<dyn Storage>,
) {
    let mut storage = factory();
    let schema_a = SchemaHash::from_bytes([0x11; 32]);
    let schema_b = SchemaHash::from_bytes([0x22; 32]);
    let shared_batch = BatchId::parse_segment("b0000000000000000000000000000002a").unwrap();

    let branch_a = crate::query_manager::types::BranchPrefixName::new("dev", schema_a, "main")
        .with_batch_id(shared_batch)
        .to_branch_name();
    let branch_b = crate::query_manager::types::BranchPrefixName::new("dev", schema_b, "main")
        .with_batch_id(shared_batch)
        .to_branch_name();

    let row_a = ObjectId::new();
    let row_b = ObjectId::new();

    storage
        .compat_index_insert(
            "users",
            "name",
            branch_a,
            &Value::Text("alice".to_string()),
            row_a,
        )
        .unwrap();
    storage
        .compat_index_insert(
            "users",
            "name",
            branch_b,
            &Value::Text("alice".to_string()),
            row_b,
        )
        .unwrap();

    let results_a =
        storage.compat_index_lookup("users", "name", branch_a, &Value::Text("alice".to_string()));
    assert_eq!(results_a, vec![row_a]);

    let results_b =
        storage.compat_index_lookup("users", "name", branch_b, &Value::Text("alice".to_string()));
    assert_eq!(results_b, vec![row_b]);
}

// ============================================================================
// Macros
// ============================================================================

/// Generate `#[test]` functions for all non-persistence conformance tests.
///
/// Usage:
/// ```ignore
/// storage_conformance_tests!(memory, || Box::new(MemoryStorage::default()));
/// ```
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
            fn branch_load_nonexistent_returns_none() {
                conformance::test_branch_load_nonexistent_returns_none(&$factory);
            }

            #[test]
            fn commit_append_and_load() {
                conformance::test_commit_append_and_load(&$factory);
            }

            #[test]
            fn commit_append_chain() {
                conformance::test_commit_append_chain(&$factory);
            }

            #[test]
            fn commit_delete() {
                conformance::test_commit_delete(&$factory);
            }

            #[test]
            fn branch_tails_set_and_clear() {
                conformance::test_branch_tails_set_and_clear(&$factory);
            }

            #[test]
            fn multiple_branches_independent() {
                conformance::test_multiple_branches_independent(&$factory);
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
            fn index_range_inclusive_exclusive() {
                conformance::test_index_range_inclusive_exclusive(&$factory);
            }

            #[test]
            fn index_range_unbounded_start() {
                conformance::test_index_range_unbounded_start(&$factory);
            }

            #[test]
            fn index_range_unbounded_end() {
                conformance::test_index_range_unbounded_end(&$factory);
            }

            #[test]
            fn index_scan_all() {
                conformance::test_index_scan_all(&$factory);
            }

            #[test]
            fn index_cross_table_isolation() {
                conformance::test_index_cross_table_isolation(&$factory);
            }

            #[test]
            fn index_cross_branch_isolation() {
                conformance::test_index_cross_branch_isolation(&$factory);
            }

            #[test]
            fn index_composed_prefix_isolation_with_same_batch_id() {
                conformance::test_index_composed_prefix_isolation_with_same_batch_id(&$factory);
            }

            #[test]
            fn index_value_types() {
                conformance::test_index_value_types(&$factory);
            }

            #[test]
            fn store_and_load_ack_tier() {
                conformance::test_store_and_load_ack_tier(&$factory);
            }

            #[test]
            fn catalogue_manifest_schema_seen() {
                conformance::test_catalogue_manifest_schema_seen(&$factory);
            }

            #[test]
            fn catalogue_manifest_lens_seen() {
                conformance::test_catalogue_manifest_lens_seen(&$factory);
            }

            #[test]
            fn catalogue_manifest_idempotent() {
                conformance::test_catalogue_manifest_idempotent(&$factory);
            }

            #[test]
            fn catalogue_manifest_nonexistent_returns_none() {
                conformance::test_catalogue_manifest_nonexistent_returns_none(&$factory);
            }

            #[test]
            fn alice_bob_concurrent_branches() {
                conformance::test_alice_bob_concurrent_branches(&$factory);
            }
        }
    };
}

/// Generate all conformance tests including persistence tests.
///
/// Usage:
/// ```ignore
/// storage_conformance_tests_persistent!(
///     fjall,
///     || Box::new(FjallStorage::open_temp().unwrap()),
///     |path| Box::new(FjallStorage::open(path).unwrap())
/// );
/// ```
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
