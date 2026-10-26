//! Trusted account-host handoff preserves original write identities and fences pending uploads.
mod common;
use common::{allow_all_policies, compile_schema};
use jazz::account_registry::AccountId;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SyncMessage, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchema};
use jazz::tx::Fate;
use std::{collections::BTreeMap, fs::File, path::PathBuf};

fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://handoff.example", name)
        .unwrap()
        .with_account(AccountId(RowUuid::from_bytes([7; 16]).0))
}
fn config(who: AuthorSubject, node: u8) -> DbConfig {
    DbConfig::new(
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchema::builder("items")
                        .column("text", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: who,
        },
    )
}
struct Root {
    _directory: tempfile::TempDir,
    _lock: File,
    path: PathBuf,
}
impl Root {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let lock = File::create(directory.path().join("owner.lock")).unwrap();
        lock.lock().unwrap();
        let path = directory.path().join("owner.sqlite");
        Self {
            _directory: directory,
            _lock: lock,
            path,
        }
    }
    fn open(&self, who: AuthorSubject) -> Db {
        let db = block_on(Db::open_at(config(who, 1), &self.path, Default::default())).unwrap();
        db.set_scope_isolated_storage_owner().unwrap();
        db
    }
    fn handoff(&self, who: AuthorSubject) -> Result<Db, jazz::db::Error> {
        // SAFETY: this fixture is the trusted account host; Root owns the
        // exclusive lock and callers close/drop every preceding Db first.
        block_on(unsafe {
            Db::open_at_as_account_owner(config(who, 1), &self.path, Default::default())
        })
    }
}
fn close(db: Db) {
    block_on(db.close()).unwrap();
    drop(db);
}
fn connect(client: &Db, server: &Db, who: AuthorSubject) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, who);
}
fn drive(nodes: &[&Db]) {
    for _ in 0..20 {
        for db in nodes {
            block_on(db.tick()).unwrap();
        }
    }
}
fn insert(db: &Db, text: &str) -> (RowUuid, u64) {
    let write = block_on(db.insert(
        "items",
        BTreeMap::from([("text".into(), Value::String(text.into()))]),
        Default::default(),
    ))
    .unwrap();
    (write.row_uuid(), write.mergeable_tx_id())
}
fn texts(db: &Db) -> Vec<String> {
    block_on(db.all(
        &db.prepare_query(&Query::from("items")).unwrap(),
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
    ))
    .unwrap()
    .into_iter()
    .map(|row| {
        let Some(Value::String(text)) = row.cells().find(|(name, _)| *name == "text").unwrap().1
        else {
            panic!("text")
        };
        text.clone()
    })
    .collect()
}
fn pending_wire(db: &Db) -> Vec<u8> {
    let (up, mut peer) = duplex();
    block_on(db.connect_upstream(up));
    drive(&[db]);
    let commit = std::iter::from_fn(|| peer.try_recv())
        .find(|message| matches!(message, SyncMessage::Commit { .. }))
        .expect("pending upload");
    postcard::to_allocvec(&commit).unwrap()
}

/// Alice drains her private owner before linked Bob takes over through the
/// account host. Normal opens cannot perform the handoff. Accepted rows and
/// write attribution remain Alice's, and Bob can publish under his own identity.
#[test]
fn drained_account_handoff_preserves_history_and_write_identity() {
    let root = Root::new();
    let server = block_on(Db::open_history_complete(config(AuthorSubject::SYSTEM, 9))).unwrap();
    let alice = root.open(author("alice"));
    connect(&alice, &server, author("alice"));
    let (id, tx) = insert(&alice, "alice");
    drive(&[&alice, &server]);
    assert_eq!(alice.write_state(tx).unwrap().fate, Fate::Accepted);
    let provenance = alice.row_provenance("items", id).unwrap();
    close(alice);
    assert!(
        block_on(Db::open_at(
            config(author("bob"), 1),
            &root.path,
            Default::default()
        ))
        .is_err()
    );
    let bob = root.handoff(author("bob")).unwrap();
    assert_eq!(texts(&bob), ["alice"]);
    assert_eq!(bob.row_provenance("items", id).unwrap(), provenance);
    connect(&bob, &server, author("bob"));
    let (_, tx) = insert(&bob, "bob");
    drive(&[&bob, &server]);
    assert_eq!(bob.write_state(tx).unwrap().fate, Fate::Accepted);
    close(bob);
    let alice = root.handoff(author("alice")).unwrap();
    let mut rows = texts(&alice);
    rows.sort();
    assert_eq!(rows, ["alice", "bob"]);
    close(alice);
}

/// Alice's unsent unit blocks linked Bob even through the trusted account host.
/// Failed handoff leaves the old login reopenable and its exact original wire
/// upload unchanged, including node, transaction ID, author and session binding.
#[test]
fn pending_account_handoff_preserves_original_upload_and_owner() {
    let root = Root::new();
    let alice = root.open(author("alice"));
    insert(&alice, "offline");
    let before = pending_wire(&alice);
    close(alice);
    let error = match root.handoff(author("bob")) {
        Ok(_) => panic!("pending handoff"),
        Err(error) => error,
    };
    assert!(
        error.message.contains("original uploads are pending"),
        "{error:?}"
    );
    let alice = root.open(author("alice"));
    assert_eq!(texts(&alice), ["offline"]);
    assert_eq!(pending_wire(&alice), before);
    close(alice);
}

/// Alice's sibling has a pending upload in the private owner's relay journal.
/// Closing both handles does not make it settled: linked Bob cannot adopt the
/// root, and Alice's later reopen recovers the sibling's original transaction.
#[test]
fn pending_foreground_upload_blocks_linked_owner_handoff() {
    let root = Root::new();
    let alice = root.open(author("alice"));
    let foreground = block_on(Db::open(config(author("alice"), 2))).unwrap();
    foreground.set_non_durable_client();
    connect(&foreground, &alice, author("alice"));
    drive(&[&foreground, &alice]);
    insert(&foreground, "sibling");
    drive(&[&foreground, &alice]);
    let before = pending_wire(&alice);
    close(foreground);
    close(alice);
    assert!(root.handoff(author("bob")).is_err());
    let alice = root.open(author("alice"));
    assert_eq!(texts(&alice), ["sibling"]);
    assert_eq!(pending_wire(&alice), before);
    close(alice);
}

/// Mallory cannot cross accounts or replace a private account owner with a
/// system or accountless subject. Rejections keep Alice's original root usable.
#[test]
fn account_handoff_rejects_unlinked_subjects() {
    let root = Root::new();
    close(root.open(author("alice")));
    for who in [
        author("mallory").with_account(AccountId(RowUuid::from_bytes([8; 16]).0)),
        AuthorSubject::authenticated("https://handoff.example", "mallory").unwrap(),
        AuthorSubject::SYSTEM,
    ] {
        assert!(root.handoff(who).is_err());
        close(root.open(author("alice")));
    }
}
