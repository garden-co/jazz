//! A catalogue schema version this build cannot read (a column type or
//! other variant from a newer Jazz) fails closed on its own: the receiver
//! skips that version with `SchemaVersionUnsupported` and keeps syncing the
//! versions it knows (and rows written under the skipped one), instead of
//! failing to decode the whole catalogue message and dropping the link with
//! it. A corrupt version still fails the message.

use jazz_types::legacy_test_future::ResultFutureExt;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use jazz::db::{
    CatalogueCell, CatalogueSchema, CatalogueUpdate, CatalogueVersion, Db, DbConfig, DbIdentity,
    InsertOptions, ReadOpts, Reason, SyncCell, SyncMessage, Transport, TransportError, block_on,
    decode_message, encode_frames,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid, SchemaVersionId};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// `schema()` plus a nullable `note`: a version bob mirrors.
fn with_note() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .nullable_column("note", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn add_note() -> MigrationLens {
    MigrationLens::new(
        schema().version_id(),
        with_note().version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: vec![LensOp::AddColumn {
                column: "note".into(),
                default: Value::Nullable(None),
            }],
        }],
    )
    .unwrap()
}

fn open(node: u8) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

/// The id of the version a newer authority added.
fn newer() -> SchemaVersionId {
    SchemaVersionId::from_bytes([0x77; 16])
}

/// What a newer authority sends: a version whose public schema JSON names
/// a column type this build has no variant for (`notes.where`, as this
/// schema's JSON writes it, with its type renamed).
fn newer_version() -> CatalogueVersion {
    let with_where = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .column("where", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let json = serde_json::to_string(with_where.public_schema())
        .unwrap()
        .replace(r#"{"type":"Bytea"}"#, r#"{"type":"Hologram","sides":3}"#);
    assert!(json.contains("Hologram"), "{json}");
    CatalogueVersion {
        schema: CatalogueSchema::Unsupported {
            id: newer(),
            json: json.into_bytes(),
            error: String::new(),
        },
        names: vec![(
            "notes".into(),
            "notes".into(),
            vec![("where".into(), "where".into())],
        )],
    }
}

/// A link whose catalogues carry the newer version too, every message
/// passed through the wire encoding as a real link would.
struct NewerAuthority {
    inner: Box<dyn Transport>,
}

impl Transport for NewerAuthority {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let mut message = self.inner.try_recv()?;
        // The newer version, and a lens cell of it: both skipped.
        let add = |catalogue: &mut CatalogueUpdate| {
            catalogue.versions.push(newer_version());
            catalogue.cells.push(CatalogueCell {
                table: "notes".into(),
                column: "where".into(),
                since: *newer().0.as_bytes(),
                source: Some("title".into()),
                default: SyncCell::Null,
            });
        };
        match &mut message {
            SyncMessage::Catalogue(catalogue) => add(catalogue),
            SyncMessage::Welcome { catalogue, .. } => add(catalogue),
            _ => {}
        }
        let frames = encode_frames(message).unwrap();
        assert_eq!(frames.len(), 1);
        Some(decode_message(&frames[0]).expect("one undecodable version decodes"))
    }

    fn features(&self) -> jazz::wire::WireFeatures {
        self.inner.features()
    }
}

/// The version's bytes are those of any version: decoding marks it
/// unsupported, with why, and encoding it again gives the same bytes.
#[test]
fn an_undecodable_version_decodes_as_unsupported_and_keeps_its_bytes() {
    let message = SyncMessage::Catalogue(CatalogueUpdate {
        namespace: None,
        versions: vec![newer_version()],
        cells: Vec::new(),
        defaults: Vec::new(),
        confirmed: Vec::new(),
    });
    let bytes = encode_frames(message).unwrap().remove(0);
    let decoded = decode_message(&bytes).unwrap();
    let SyncMessage::Catalogue(catalogue) = &decoded else {
        panic!("{decoded:?}");
    };
    let CatalogueSchema::Unsupported { id, error, .. } = &catalogue.versions[0].schema else {
        panic!("{:?}", catalogue.versions[0].schema);
    };
    assert_eq!(*id, newer());
    assert!(error.contains("Hologram"), "{error}");
    assert_eq!(encode_frames(decoded).unwrap().remove(0), bytes);
}

/// Bob's store syncs from an authority whose catalogue also holds a version
/// it cannot read: it skips that version (`schema_version_unsupported`),
/// keeps its link, and keeps syncing the rows of the version it knows.
///
/// ```
/// server ──Welcome{v1, v-newer}──► bob   v-newer: skipped, recorded
/// server ──row "first"──────────► bob   synced as before
/// ```
#[test]
fn a_store_skips_a_version_it_cannot_read_and_keeps_syncing() {
    let server = open(0);
    jazz::db::block_on(server.set_authority()).unwrap();
    // A version bob lacks and mirrors, beside the one he cannot read.
    block_on(server.admit_schema_with_lenses(with_note(), &[add_note()])).unwrap();
    let bob = open(1);
    let (up, down) = jazz::db::duplex();
    let _link = block_on(bob.connect_upstream(Box::new(NewerAuthority { inner: up })));
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    let all = bob.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(bob.subscribe(&all, ReadOpts::default())).unwrap();
    let pump = || {
        for _ in 0..8 {
            block_on(bob.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    pump();
    block_on(server.insert(
        "notes",
        BTreeMap::from([("title".to_owned(), Value::String("first".into()))]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([1; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    pump();
    let rows = block_on(bob.all(&all, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1, "the link stayed up and the row synced");
    let skipped = bob.unsupported_schema_versions();
    assert_eq!(skipped.len(), 1, "{skipped:?}");
    assert_eq!(skipped[0].0, newer());
    assert_eq!(skipped[0].1.reason, Some(Reason::SchemaVersionUnsupported));
    assert!(
        skipped[0].1.message.contains("Hologram"),
        "{}",
        skipped[0].1
    );

    // Its lens cell was skipped with it: a replica behind bob is told of
    // none (a relay passes on the cells it keeps).
    let carol = open(2);
    let cells = Rc::new(RefCell::new(Vec::new()));
    let (up, down) = jazz::db::duplex();
    let _carol_link = block_on(carol.connect_upstream(Box::new(Cells {
        inner: up,
        cells: cells.clone(),
    })));
    bob.accept_subscriber(down, AuthorSubject::SYSTEM).unwrap();
    for _ in 0..8 {
        block_on(carol.tick()).unwrap();
        block_on(bob.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(
        !cells
            .borrow()
            .iter()
            .any(|since| *since == *newer().0.as_bytes()),
        "{:?}",
        cells.borrow()
    );
}

/// A link that records the `since` of every lens cell its catalogues name.
struct Cells {
    inner: Box<dyn Transport>,
    cells: Rc<RefCell<Vec<[u8; 16]>>>,
}

impl Transport for Cells {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let message = self.inner.try_recv()?;
        if let SyncMessage::Catalogue(catalogue) | SyncMessage::Welcome { catalogue, .. } = &message
        {
            self.cells
                .borrow_mut()
                .extend(catalogue.cells.iter().map(|cell| cell.since));
        }
        Some(message)
    }

    fn features(&self) -> jazz::wire::WireFeatures {
        self.inner.features()
    }
}

/// A catalogue holding one version `id` whose public schema JSON is `json`,
/// as bytes on the wire.
fn catalogue_bytes(id: SchemaVersionId, json: &str) -> Vec<u8> {
    let mut version = newer_version();
    version.schema = CatalogueSchema::Unsupported {
        id,
        json: json.as_bytes().to_vec(),
        error: String::new(),
    };
    encode_frames(SyncMessage::Catalogue(CatalogueUpdate {
        namespace: None,
        versions: vec![version],
        cells: Vec::new(),
        defaults: Vec::new(),
        confirmed: Vec::new(),
    }))
    .unwrap()
    .remove(0)
}

/// Only a shape this build does not know (a variant from a newer Jazz)
/// makes a version unsupported. A corrupt one (bad JSON, truncated
/// bytes, a wrong type for a known key, or a known shape that does not
/// compile) fails the message, as it always has.
#[test]
fn only_a_newer_shape_is_unsupported_and_a_corrupt_version_fails() {
    let valid = serde_json::to_string(schema().public_schema()).unwrap();
    let id = schema().version_id();
    let unsupported = |id: SchemaVersionId, json: &str| {
        let decoded = decode_message(&catalogue_bytes(id, json));
        match &decoded {
            Ok(SyncMessage::Catalogue(c)) => match &c.versions[0].schema {
                CatalogueSchema::Unsupported { error, .. } => error.clone(),
                CatalogueSchema::Known(_) => panic!("read as known: {json}"),
            },
            _ => panic!("{json}: {decoded:?}"),
        }
    };
    assert!(matches!(
        decode_message(&catalogue_bytes(id, &valid)),
        Ok(SyncMessage::Catalogue(ref c)) if matches!(c.versions[0].schema, CatalogueSchema::Known(_))
    ));
    let newer_variant = valid.replacen(r#"{"type":"Text"}"#, r#"{"type":"Hologram"}"#, 1);
    assert!(unsupported(newer(), &newer_variant).contains("Hologram"));
    // A key this build ignores, which the sender's id counts (a newer
    // constraint): read without it, the version would be this build's own
    // `schema()`, under an id the sender never wrote. It is not read.
    let newer_key = valid.replacen(r#""nullable":false"#, r#""nullable":false,"maxBytes":4"#, 1);
    let error = unsupported(newer(), &newer_key);
    assert!(error.contains("keys this build ignores"), "{error}");
    assert!(error.contains(&id.0.to_string()), "{error}");
    // The same key under the id this build computes adds nothing it reads.
    assert!(matches!(
        decode_message(&catalogue_bytes(id, &newer_key)),
        Ok(SyncMessage::Catalogue(ref c)) if matches!(c.versions[0].schema, CatalogueSchema::Known(_))
    ));
    let syntax = valid.replacen('{', "{,", 1);
    let truncated = valid[..valid.len() - 3].to_owned();
    let wrong_type = valid.replacen(r#""nullable":false"#, r#""nullable":"no""#, 1);
    let uncompilable = valid.replacen(
        r#""nullable":false"#,
        r#""nullable":false,"references":"nowhere""#,
        1,
    );
    for (case, json) in [
        ("syntax", &syntax),
        ("truncated", &truncated),
        ("wrong type", &wrong_type),
        ("does not compile", &uncompilable),
    ] {
        assert!(
            decode_message(&catalogue_bytes(id, json)).is_err(),
            "{case}"
        );
    }
    // The last is well-formed: the compiler, not the decoder, refuses it.
    let source: jazz::tools::Schema = serde_json::from_str(&uncompilable).unwrap();
    assert!(JazzSchema::new(&source).is_err());
}

/// `schema()` plus a nullable `where`: what a newer Jazz writes with a
/// column type this build lacks (on the wire, `NewerType` renames it).
fn with_where() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .nullable_column("where", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// A link that renames `with_where()`'s `where` type in every catalogue to
/// one this build lacks, as a newer authority would send it.
struct NewerType {
    inner: Box<dyn Transport>,
}

impl Transport for NewerType {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let mut message = self.inner.try_recv()?;
        if let SyncMessage::Catalogue(catalogue) | SyncMessage::Welcome { catalogue, .. } =
            &mut message
        {
            for version in &mut catalogue.versions {
                if version.schema.id() == with_where().version_id() {
                    let json = serde_json::to_string(with_where().public_schema())
                        .unwrap()
                        .replace(r#"{"type":"Bytea"}"#, r#"{"type":"Hologram"}"#);
                    version.schema = CatalogueSchema::Unsupported {
                        id: with_where().version_id(),
                        json: json.into_bytes(),
                        error: String::new(),
                    };
                }
            }
        }
        let frames = encode_frames(message).unwrap();
        Some(decode_message(&frames[0]).unwrap())
    }

    fn features(&self) -> jazz::wire::WireFeatures {
        self.inner.features()
    }
}

/// The authority writes under a version bob cannot read (v2 adds a column
/// of a type his build lacks). Bob skips v2 and keeps his link; v2's rows
/// reach him with the columns he knows, and nothing errors, row after row.
///
/// ```
/// server (v1 → v2: + where: Hologram) ──row 1, row 2 (v2)──► bob (v1)
///   bob: v2 skipped; rows 1, 2 with their titles; link up
/// ```
#[test]
fn rows_written_under_a_skipped_version_reach_an_older_replica() {
    let add_where = MigrationLens::new(
        schema().version_id(),
        with_where().version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: vec![LensOp::AddColumn {
                column: "where".into(),
                default: Value::Nullable(None),
            }],
        }],
    )
    .unwrap();
    // The server is deployed at v2: it opens under it, shipping v1 and the
    // lens into v2, and installs it before any write.
    let server = block_on(Db::open(
        DbConfig::new(
            with_where(),
            DbIdentity {
                node: NodeUuid::from_bytes([0; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_migrations(vec![schema()], vec![add_where]),
    ))
    .unwrap();
    jazz::db::block_on(server.set_authority()).unwrap();
    let bob = open(1);
    let (up, down) = jazz::db::duplex();
    let link = block_on(bob.connect_upstream(Box::new(NewerType { inner: up })));
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    let all = bob.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(bob.subscribe(&all, ReadOpts::default())).unwrap();
    let pump = || {
        for _ in 0..8 {
            block_on(bob.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    let write = |n: u8, title: &str| {
        block_on(server.insert(
            "notes",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.into())),
                (
                    "where".to_owned(),
                    Value::Nullable(Some(Box::new(Value::Bytes(vec![n; 8])))),
                ),
            ]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
    };
    pump();
    write(1, "first");
    pump();
    write(2, "second");
    pump();

    assert_eq!(
        bob.unsupported_schema_versions()
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        [with_where().version_id()]
    );
    assert!(bob.upstream_refusal(&link).is_none());
    let rows = block_on(bob.all(&all, ReadOpts::default())).unwrap();
    let titles: Vec<_> = rows
        .iter()
        .map(|row| {
            row.cells()
                .find(|(column, _)| *column == "title")
                .and_then(|(_, value)| value.cloned())
                .unwrap()
        })
        .collect();
    assert_eq!(
        titles,
        [
            Value::String("first".into()),
            Value::String("second".into())
        ]
    );
}
