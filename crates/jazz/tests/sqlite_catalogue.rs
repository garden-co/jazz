//! Accepted UUIDs come from the published authority catalogue, never from
//! local schema names, query registration or an optimistic replica proposal.
mod common;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, RemoteLinkHint, SyncMessage, Transport, TransportError,
    block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, LensOp, MigrationLens, PhysicalIdentityManifest,
    SchemaLineagePublication, SchemaVersion, TableLens,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    path::Path,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};
fn schema(table: &str, column: &str) -> JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column(column, ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    )
}
fn genesis(schema: JazzSchema) -> CatalogueSnapshot {
    CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&schema),
        schemas: vec![SchemaVersion::new(schema.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: schema.version_id(),
        },
    }
}
fn open(node: u8, schema: JazzSchema, path: Option<&Path>) -> Db {
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}
fn authority(node: u8, snapshot: &CatalogueSnapshot, path: Option<&Path>) -> Db {
    let db = open(node, snapshot.schemas[0].schema.clone(), path);
    db.set_authority();
    db.install_authority_catalogue(snapshot).unwrap();
    db
}
struct Capture {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
}
impl Transport for Capture {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push(message.clone());
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
fn connect(
    client: &Db,
    server: &Db,
    who: AuthorSubject,
    claims: Option<BTreeMap<String, Value>>,
) -> Rc<RefCell<Vec<SyncMessage>>> {
    let (a, b) = duplex();
    let sent = Rc::new(RefCell::new(vec![]));
    block_on(client.connect_upstream(a));
    server.accept_subscriber_with_claims(
        Box::new(Capture {
            inner: b,
            sent: sent.clone(),
        }),
        who,
        claims,
    );
    sent
}
fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(v) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return v;
        }
        turn(dbs);
    }
    panic!("read did not finish")
}
fn user(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn claims(value: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        jazz::query::provider_claim_key("access"),
        Value::String(value.into()),
    )])
}
fn rename(snapshot: &mut CatalogueSnapshot, table: &str, column: &str) -> MigrationLens {
    let source = snapshot.schemas.last().unwrap().schema.clone();
    let source_table = &source.tables()[0];
    let from = &source_table.name;
    let from_col = &source_table.columns[0].name;
    let target = schema(table, column);
    let lens = MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: from.clone(),
            target_table: table.into(),
            ops: if from_col != column {
                vec![LensOp::RenameColumn {
                    from: from_col.clone(),
                    to: column.into(),
                }]
            } else {
                vec![]
            },
        }],
    )
    .unwrap();
    let identities = snapshot
        .lineages
        .last()
        .map_or(&snapshot.genesis_physical_identities, |(_, p)| {
            &p.physical_identities
        });
    let history = std::iter::once(snapshot.genesis_physical_identities.clone()).chain(
        snapshot
            .lineages
            .iter()
            .map(|(_, p)| p.physical_identities.clone()),
    );
    let publication = SchemaLineagePublication::author_from_prior_with_history(
        &source,
        identities,
        history,
        SchemaVersion::new(target.clone()),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot
        .lineages
        .push((snapshot.lineages.len() as u64 + 1, publication));
    snapshot.schemas.push(SchemaVersion::new(target.clone()));
    snapshot.current_write_schema = CurrentWriteSchema {
        revision: snapshot.current_write_schema.revision + 1,
        schema: target.version_id(),
    };
    lens
}

/// Alice's two independent apps use the same schema but distinct minted IDs.
/// Bob's unpublished local schema and typed view must never allocate accepted IDs.
#[test]
fn identities_are_authority_issued_and_lookup_never_initializes() {
    let s = schema("notes", "text");
    let a = genesis(s.clone());
    let b = genesis(s.clone());
    let alice = authority(3, &a, None);
    let bob = authority(4, &b, None);
    assert_ne!(
        alice.table_identity("notes").unwrap(),
        bob.table_identity("notes").unwrap()
    );
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        Some(a.genesis_physical_identities.tables["notes"].id)
    );
    assert_eq!(
        alice.column_identity("notes", "text").unwrap(),
        Some(a.genesis_physical_identities.tables["notes"].columns["text"].id)
    );
    assert!(alice.table_identity("unknown").unwrap().is_none());
    assert!(alice.column_identity("notes", "unknown").unwrap().is_none());
    let local = open(1, s, None);
    assert_eq!(
        local.table_identity("notes").unwrap_err().code,
        ErrorCode::NotObserved
    );
    assert!(local.install_authority_catalogue(&a).is_err());
    let view = block_on(alice.register_schema_view(schema("drafts", "text"))).unwrap();
    assert!(view.table_identity("drafts").unwrap().is_none());
}

/// Alice learns IDs via ordinary sync metadata without downloading row bodies.
/// Offline lookup remains available; each fresh coverage request needs authority.
#[test]
fn cold_coverage_and_offline_lookup_have_distinct_contracts() {
    let snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    block_on(server.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("private body".into()))]),
        Default::default(),
    ))
    .unwrap();
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let sent = connect(&alice, &server, user("alice"), Some(claims("alice")));
    drive(
        &[&alice, &server],
        alice.cover_catalogue("notes", Some((user("alice"), claims("alice")))),
    )
    .unwrap();
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
    assert!(
        !sent
            .borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::Rows { .. }))
    );
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(
        alice.column_identity("notes", "text").unwrap(),
        server.column_identity("notes", "text").unwrap()
    );
    assert_eq!(
        block_on(alice.cover_catalogue("notes", None))
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
}

/// Bob's relay forwards Alice's fresh coverage to the authority, retaining exact
/// UUIDs. Its cached catalogue cannot satisfy fresh coverage after upstream loss.
#[test]
fn cold_relay_coverage_reaches_authority() {
    let snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    let relay = open(2, snapshot.schemas[0].schema.clone(), None);
    connect(&relay, &server, AuthorSubject::SYSTEM, None);
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    connect(&alice, &relay, user("alice"), Some(claims("alice")));
    drive(
        &[&alice, &relay, &server],
        alice.cover_catalogue("notes", Some((user("alice"), claims("alice")))),
    )
    .unwrap();
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(
        drive(&[&alice, &relay], alice.cover_catalogue("notes", None))
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
}

/// Mallory cannot replace her authenticated author/claims in a catalogue read;
/// an explicitly trusted backend can delegate Alice's exact request scope.
#[test]
fn catalogue_coverage_preserves_the_connection_scope() {
    for (connection, requested, allow) in [
        (user("alice"), claims("alice"), true),
        (user("alice"), claims("bob"), false),
        (user("mallory"), claims("alice"), false),
        (AuthorSubject::SYSTEM, claims("bob"), true),
    ] {
        let snapshot = genesis(schema("notes", "text"));
        let server = authority(3, &snapshot, None);
        let alice = open(1, snapshot.schemas[0].schema.clone(), None);
        connect(&alice, &server, connection, Some(claims("alice")));
        let result = drive(
            &[&alice, &server],
            alice.cover_catalogue("notes", Some((user("alice"), requested))),
        );
        if allow {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::WriteRejected);
        }
    }
}

/// Alice's owner follows the current accepted descriptor across table/column
/// renames; her fixed old view keeps its original names and the same UUIDs.
#[test]
fn renames_and_fixed_views_preserve_published_identities() {
    let mut snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let old = block_on(alice.register_schema_view(snapshot.schemas[0].schema.clone())).unwrap();
    connect(&alice, &server, AuthorSubject::SYSTEM, None);
    drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
    let table = alice.table_identity("notes").unwrap();
    let column = alice.column_identity("notes", "text").unwrap();
    let lens = rename(&mut snapshot, "tasks", "body");
    block_on(
        server
            .activate_schema_with_lenses(snapshot.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    server.install_authority_catalogue(&snapshot).unwrap();
    drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
    assert_eq!(alice.table_identity("tasks").unwrap(), table);
    assert_eq!(alice.column_identity("tasks", "body").unwrap(), column);
    assert!(alice.table_identity("notes").unwrap().is_none());
    assert_eq!(old.table_identity("notes").unwrap(), table);
    assert_eq!(old.column_identity("notes", "text").unwrap(), column);
    assert!(old.table_identity("tasks").unwrap().is_none());
}

/// After an offline restart Alice can resolve the same accepted UUIDs without
/// contacting Bob. Reinitialising the authority also keeps its durable genesis.
#[test]
fn accepted_catalogue_survives_replica_and_authority_restart() {
    let snapshot = genesis(schema("notes", "text"));
    let dir = tempfile::tempdir().unwrap();
    let server_path = dir.path().join("server.sqlite");
    let path = dir.path().join("alice.sqlite");
    let expected = snapshot.genesis_physical_identities.tables["notes"].id;
    {
        let server = authority(3, &snapshot, Some(&server_path));
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        connect(&alice, &server, AuthorSubject::SYSTEM, None);
        drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
    }
    let server = open(3, snapshot.schemas[0].schema.clone(), Some(&server_path));
    server.set_authority();
    server.initialize_authority_catalogue().unwrap();
    assert_eq!(server.table_identity("notes").unwrap(), Some(expected));
    let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(alice.table_identity("notes").unwrap(), Some(expected));
}

/// A schema with forged, reused, or nil IDs is not a new accepted publication.
/// Bob's previously accepted metadata must survive every rejected installation.
#[test]
fn invalid_catalogue_installation_preserves_the_previous_head() {
    let first = genesis(schema("notes", "text"));
    let db = authority(3, &first, None);
    let expected = db.table_identity("notes").unwrap();
    for case in ["nil", "rebind", "wrong_schema", "missing_history"] {
        let mut bad = first.clone();
        match case {
            "nil" => {
                bad.genesis_physical_identities
                    .tables
                    .get_mut("notes")
                    .unwrap()
                    .id
                    .0 = uuid::Uuid::nil()
            }
            "rebind" => {
                bad.genesis_physical_identities =
                    PhysicalIdentityManifest::allocate(&bad.schemas[0].schema)
            }
            "wrong_schema" => bad.schemas[0].id = schema("other", "text").version_id(),
            _ => {
                bad.schemas.clear();
            }
        }
        assert!(db.install_authority_catalogue(&bad).is_err(), "{case}");
        assert_eq!(db.table_identity("notes").unwrap(), expected);
    }
}

/// Malformed durable bytes must not turn into plausible identity strings.
/// Direct SQL corruption is needed to exercise this storage codec boundary.
#[test]
fn corrupt_cached_catalogue_fails_closed() {
    for target in ["head", "table", "schema"] {
        let snapshot = genesis(schema("notes", "text"));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        {
            let server = authority(3, &snapshot, None);
            let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
            connect(&alice, &server, AuthorSubject::SYSTEM, None);
            drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
        }
        let sql = rusqlite::Connection::open(&path).unwrap();
        let name = format!("_jazz_catalogue_{target}_v1");
        sql.execute(&format!("UPDATE {name} SET value=x'ff'"), [])
            .unwrap();
        drop(sql);
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        if target == "schema" {
            let server = authority(3, &snapshot, None);
            connect(&alice, &server, AuthorSubject::SYSTEM, None);
            block_on(alice.tick()).unwrap();
            block_on(server.tick()).unwrap();
            assert_eq!(block_on(alice.tick()).unwrap_err().code, ErrorCode::Storage);
        } else {
            assert_eq!(
                alice.table_identity("notes").unwrap_err().code,
                ErrorCode::Storage
            );
        }
    }
}

/// Alice must not complete coverage from an answer whose metadata rolled back.
/// The synthetic upstream is needed to place a corrupt frame in the same tick.
#[test]
fn rolled_back_metadata_cannot_complete_a_coverage_request() {
    let snapshot = genesis(schema("notes", "text"));
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let (a, mut peer) = duplex();
    block_on(alice.connect_upstream(a));
    let mut read = pin!(alice.cover_catalogue("notes", None));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    block_on(alice.tick()).unwrap();
    let mut id = None;
    while let Some(message) = peer.try_recv() {
        if let SyncMessage::CatalogueFetchV1 { id: request, .. } = message {
            id = Some(request);
        }
    }
    let head = jazz::db::CatalogueHeadV1 {
        root: [7; 32],
        through: 0,
        revision: 1,
        current: snapshot.schemas[0].id,
    };
    peer.send(SyncMessage::CatalogueMetadataV1 {
        head: head.clone(),
        schemas: vec![jazz::db::CatalogueSchemaV1 {
            schema: snapshot.schemas[0].clone(),
            identities: snapshot.genesis_physical_identities.clone(),
        }],
    })
    .unwrap();
    peer.send(SyncMessage::CatalogueCoveredV1 {
        id: id.unwrap(),
        head: head.clone(),
        opened: head.current,
    })
    .unwrap();
    peer.send(SyncMessage::CatalogueMetadataV1 {
        head: jazz::db::CatalogueHeadV1 {
            root: [0; 32],
            ..head
        },
        schemas: vec![],
    })
    .unwrap();
    assert_eq!(
        block_on(alice.tick()).unwrap_err().code,
        ErrorCode::Protocol
    );
    assert_eq!(
        alice.table_identity("notes").unwrap_err().code,
        ErrorCode::NotObserved
    );
    match read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(error)) => assert_eq!(error.code, ErrorCode::NotObserved),
        _ => panic!("rolled-back catalogue answer remained usable"),
    }
}

/// Cancelling before send removes Alice's request; reconnecting cannot transfer
/// an in-flight request to a different authenticated upstream session.
#[test]
fn cancelled_and_reconnected_reads_do_not_inherit_coverage() {
    let alice = open(1, schema("notes", "text"), None);
    let (a, mut peer) = duplex();
    block_on(alice.connect_upstream(a));
    {
        let mut read = pin!(alice.cover_catalogue("notes", None));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    block_on(alice.tick()).unwrap();
    while let Some(message) = peer.try_recv() {
        assert!(!matches!(message, SyncMessage::CatalogueFetchV1 { .. }));
    }
    let mut read = pin!(alice.cover_catalogue("notes", None));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let (a, _) = duplex();
    block_on(alice.connect_upstream(a));
    match read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(error)) => assert_eq!(error.code, ErrorCode::NotObserved),
        _ => panic!("old request survived reconnect"),
    }
}

/// Bob's valid UUIDs cannot make an incomplete or duplicate predecessor mapping
/// acceptable. Alice retains the old accepted descriptor after every refusal.
#[test]
fn snapshot_requires_complete_and_canonical_lineage_declarations() {
    let first = genesis(schema("notes", "text"));
    let db = authority(3, &first, None);
    let mut snapshot = first.clone();
    rename(&mut snapshot, "tasks", "body");
    for case in [
        "mapped_new",
        "mapped_drop",
        "missing_map",
        "duplicate_predecessor",
        "lens_id",
    ] {
        let mut bad = snapshot.clone();
        let publication = &mut bad.lineages[0].1;
        match case {
            "mapped_new" => publication.predecessors[0].new_tables.push("tasks".into()),
            "mapped_drop" => publication.predecessors[0]
                .dropped_tables
                .push("notes".into()),
            "missing_map" => {
                publication.predecessors[0].lens = MigrationLens::new(
                    publication.predecessors[0].lens.source,
                    publication.schema.id,
                    vec![],
                )
                .unwrap()
            }
            "duplicate_predecessor" => publication
                .predecessors
                .push(publication.predecessors[0].clone()),
            _ => publication.predecessors[0].lens.id.0 = uuid::Uuid::nil(),
        }
        publication.id = publication.content_id();
        assert!(db.install_authority_catalogue(&bad).is_err(), "{case}");
        assert_eq!(
            db.table_identity("notes").unwrap(),
            Some(first.genesis_physical_identities.tables["notes"].id)
        );
    }
}

/// Removing a derived durable table record is corruption, not an unknown name.
/// Direct SQL mutation is needed to exercise the cached-storage failure path.
#[test]
fn missing_derived_table_record_is_a_storage_error() {
    let snapshot = genesis(schema("notes", "text"));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    drop(authority(3, &snapshot, Some(&path)));
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("DELETE FROM _jazz_catalogue_table_v1", [])
        .unwrap();
    let db = open(3, snapshot.schemas[0].schema.clone(), Some(&path));
    assert_eq!(
        db.table_identity("notes").unwrap_err().code,
        ErrorCode::Storage
    );
    assert_eq!(db.table_identity("unknown").unwrap(), None);
}

/// Alice's first catalogue request crosses her private owner while Bob's
/// authority policy descriptor is still arriving. Installing that descriptor
/// must not cancel the same connection's exact catalogue coverage receipt.
#[test]
fn cold_private_owner_catalogue_request_survives_policy_installation() {
    let snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    let open_private = |node| {
        block_on(Db::open(DbConfig::new(
            snapshot.schemas[0].schema.without_permissions(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: user("alice"),
            },
        )))
        .unwrap()
    };
    let owner = open_private(2);
    owner.set_scope_isolated_storage_owner().unwrap();
    let tab = open_private(1);
    tab.set_private_owner_client().unwrap();
    connect(&owner, &server, user("alice"), None);
    connect(&tab, &owner, user("alice"), None);
    drive(&[&tab, &owner, &server], tab.cover_catalogue("notes", None)).unwrap();
    assert_eq!(
        tab.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
}

/// Alice keeps the old public schema while Bob renames its column and table.
/// Her private owner must retain exact catalogue identities and compile the
/// current policy against the same accepted storage epochs, including restart.
#[test]
fn private_owner_retains_old_names_under_renamed_authority_policies() {
    use jazz::db::{ReadOpts, ReadTier};
    use jazz::query::Query;
    use jazz::tools::{PolicyExpr, TablePolicies, Value as PublicValue};
    let mut snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    for text in ["visible", "hidden"] {
        block_on(server.insert(
            "notes",
            BTreeMap::from([("text".into(), Value::String(text.into()))]),
            Default::default(),
        ))
        .unwrap();
    }
    let old_schema = snapshot.schemas[0].schema.without_permissions();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("renamed-private.sqlite");
    let open_owner = || {
        let db = block_on(Db::open_at(
            DbConfig::new(
                old_schema.clone(),
                DbIdentity {
                    node: NodeUuid::from_bytes([2; 16]),
                    author: user("alice"),
                },
            ),
            &path,
            Default::default(),
        ))
        .unwrap();
        db.set_scope_isolated_storage_owner().unwrap();
        db
    };
    let owner = open_owner();
    connect(&owner, &server, user("alice"), None);
    drive(&[&owner, &server], owner.cover_catalogue("notes", None)).unwrap();
    let query = owner.prepare_query(&Query::from("notes")).unwrap();
    let bytes = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let rows = drive(
        &[&owner, &server],
        owner.all_serialized_query_as(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            Some(user("alice")),
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let jazz::db::SerializedReadResult::Rows(rows) = rows else {
        panic!("expected rows")
    };
    assert_eq!(rows.len(), 2);
    let table = owner.table_identity("notes").unwrap();
    let column = owner.column_identity("notes", "text").unwrap();
    let lens = rename(&mut snapshot, "tasks", "body");
    let governed = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_literal(
                        "body",
                        PublicValue::Text("visible".into()),
                    ))),
            )
            .build(),
    );
    snapshot.schemas.last_mut().unwrap().schema = governed.clone();
    block_on(server.activate_schema_with_lenses(governed, &[lens])).unwrap();
    server.install_authority_catalogue(&snapshot).unwrap();
    drive(&[&owner, &server], owner.cover_catalogue("notes", None)).unwrap();
    assert_eq!(owner.table_identity("tasks").unwrap(), table);
    assert_eq!(owner.column_identity("tasks", "body").unwrap(), column);
    let rows = block_on(owner.all_for_identity(
        &query,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        user("alice"),
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cells().find(|(name, _)| *name == "text").unwrap().1,
        Some(&Value::String("visible".into()))
    );
    let tab = block_on(Db::open(DbConfig::new(
        old_schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: user("alice"),
        },
    )))
    .unwrap();
    tab.set_private_owner_client().unwrap();
    connect(&tab, &owner, user("alice"), None);
    let rows = drive(
        &[&tab, &owner, &server],
        tab.all_serialized_query_as(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::LocalFirst),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let jazz::db::SerializedReadResult::Rows(rows) = rows else {
        panic!("expected foreground rows")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cells().find(|(name, _)| *name == "text").unwrap().1,
        Some(&Value::String("visible".into()))
    );
    block_on(tab.close()).unwrap();
    block_on(owner.close()).unwrap();
    drop(owner);
    let owner = open_owner();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let query = owner.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(owner.all_for_identity(
        &query,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        user("alice"),
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cells().find(|(name, _)| *name == "text").unwrap().1,
        Some(&Value::String("visible".into()))
    );
}
