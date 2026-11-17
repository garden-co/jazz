//! A catalogue schema version this build cannot read (a column type or a
//! schema key from a newer Jazz) fails closed on its own: the receiver
//! skips that version with `SchemaVersionUnsupported` and keeps syncing the
//! versions it knows, instead of failing to decode the whole catalogue
//! message and dropping the link with it.

use std::collections::BTreeMap;

use jazz::db::{
    CatalogueSchema, CatalogueUpdate, CatalogueVersion, Db, DbConfig, DbIdentity, InsertOptions,
    ReadOpts, Reason, SyncMessage, Transport, TransportError, block_on, decode_message,
    encode_frames,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid, SchemaVersionId};
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
        let add = |catalogue: &mut CatalogueUpdate| catalogue.versions.push(newer_version());
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
/// unsupported, with why, and encoding it again gives the same bytes, so
/// a relay forwards it unchanged.
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
    server.set_authority();
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
}
