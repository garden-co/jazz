//! Authority-issued catalogue identities are separate from local schema admission.
mod common;
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, LensOp, MigrationLens, PhysicalIdentityManifest,
    SchemaLineagePublication, SchemaVersion, TableLens,
};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::path::Path;
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

/// Removing Alice's derived durable table record is corruption, not an unknown name.
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

/// Alice's authority follows the accepted current schema after a rename; Bob's
/// fixed old view retains the old names and the same physical UUIDs.
#[test]
fn authority_renames_preserve_fixed_view_identities() {
    let mut snapshot = genesis(schema("notes", "text"));
    let alice = authority(3, &snapshot, None);
    let bob = block_on(alice.register_schema_view(snapshot.schemas[0].schema.clone())).unwrap();
    let table = alice.table_identity("notes").unwrap();
    let column = alice.column_identity("notes", "text").unwrap();
    let lens = rename(&mut snapshot, "tasks", "body");
    block_on(
        alice.activate_schema_with_lenses(snapshot.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    alice.install_authority_catalogue(&snapshot).unwrap();
    assert_eq!(alice.table_identity("tasks").unwrap(), table);
    assert_eq!(alice.column_identity("tasks", "body").unwrap(), column);
    assert_eq!(alice.table_identity("notes").unwrap(), None);
    assert_eq!(bob.table_identity("notes").unwrap(), table);
    assert_eq!(bob.column_identity("notes", "text").unwrap(), column);
    assert_eq!(bob.table_identity("tasks").unwrap(), None);
}

/// Alice initializes one durable genesis. Neither a second call nor a restart
/// mints replacement UUIDs; Bob's non-authority store cannot initialize one.
#[test]
fn authority_initialization_is_explicit_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let s = schema("notes", "text");
    let alice = open(1, s.clone(), Some(&path));
    assert_eq!(
        alice.initialize_authority_catalogue().unwrap_err().code,
        ErrorCode::NotObserved
    );
    alice.set_authority();
    alice.initialize_authority_catalogue().unwrap();
    let table = alice.table_identity("notes").unwrap().unwrap();
    let column = alice.column_identity("notes", "text").unwrap().unwrap();
    alice.initialize_authority_catalogue().unwrap();
    assert_eq!(alice.table_identity("notes").unwrap(), Some(table));
    drop(alice);
    let reopened = open(1, s.clone(), Some(&path));
    reopened.set_authority();
    reopened.initialize_authority_catalogue().unwrap();
    assert_eq!(reopened.table_identity("notes").unwrap(), Some(table));
    assert_eq!(
        reopened.column_identity("notes", "text").unwrap(),
        Some(column)
    );
    let bob = open(2, s, None);
    assert_eq!(
        bob.initialize_authority_catalogue().unwrap_err().code,
        ErrorCode::NotObserved
    );
}

/// Alice cannot make her replica a publisher simply by marking it an authority
/// while Bob remains its configured upstream.
#[test]
fn attached_replica_cannot_publish_catalogue() {
    let snapshot = genesis(schema("notes", "text"));
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let (a, _bob) = jazz::db::duplex();
    block_on(alice.connect_upstream(a));
    alice.set_authority();
    assert_eq!(
        alice.initialize_authority_catalogue().unwrap_err().code,
        ErrorCode::NotObserved
    );
    assert_eq!(
        alice
            .install_authority_catalogue(&snapshot)
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
    assert_eq!(
        alice.table_identity("notes").unwrap_err().code,
        ErrorCode::NotObserved
    );
}

/// Alice's authority rejects a backwards head and Bob's incomplete history;
/// fixed views keep their previously accepted identities after either refusal.
#[test]
fn publication_is_monotone_and_cannot_forget_history() {
    let first = genesis(schema("notes", "text"));
    let alice = authority(1, &first, None);
    let old = block_on(alice.register_schema_view(first.schemas[0].schema.clone())).unwrap();
    let table = old.table_identity("notes").unwrap();
    let mut second = first.clone();
    let lens = rename(&mut second, "tasks", "body");
    block_on(
        alice.activate_schema_with_lenses(second.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    alice.install_authority_catalogue(&second).unwrap();
    assert!(alice.install_authority_catalogue(&first).is_err());
    // A newer active revision still cannot discard the already accepted lineage.
    let mut forgotten = first.clone();
    forgotten.current_write_schema.revision = second.current_write_schema.revision + 1;
    assert!(alice.install_authority_catalogue(&forgotten).is_err());
    assert_eq!(alice.table_identity("tasks").unwrap(), table);
    assert_eq!(old.table_identity("notes").unwrap(), table);
}
