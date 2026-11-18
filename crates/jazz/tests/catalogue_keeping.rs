//! A host keeps its own values (host tables) in the transaction that
//! applies a catalogue change: a key that holds a different value fails the
//! whole change, and the catalogue is as it was.

use jazz::catalogue::{CatalogueChange, CataloguePut};
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::host::{HostTable, HostTables};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

const KEPT: HostTable = HostTable::new("kept_values");

fn open() -> Db {
    let schema = jazz::schema::JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("notes").column("title", ColumnType::Text))
            .build(),
    )
    .unwrap();
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn change() -> CatalogueChange {
    let schema = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("notes").column("title", ColumnType::Text))
        .build();
    CatalogueChange {
        put: CataloguePut {
            schemas: vec![(schema, 1)],
            ..Default::default()
        },
        ..Default::default()
    }
}

/// ```text
/// apply { bundle, keep k=a } ──► bundle + k=a together
/// apply { bundle', keep k=b } ──► ✗ nothing of it applied, k stays a
/// ```
#[test]
fn a_change_keeps_host_values_with_it_or_not_at_all() {
    let db = open();
    let kept = |value: &[u8]| vec![(KEPT, b"k".to_vec(), value.to_vec())];
    block_on(db.apply_catalogue_keeping(change(), &kept(b"a"))).unwrap();
    assert_eq!(db.host_get(KEPT, b"k").unwrap(), Some(b"a".to_vec()));
    assert_eq!(db.catalogue().unwrap().schemas.len(), 1);
    // The same value again is not a conflict.
    block_on(db.apply_catalogue_keeping(change(), &kept(b"a"))).unwrap();

    let other = {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("other").column("title", ColumnType::Text))
            .build();
        CatalogueChange {
            put: CataloguePut {
                schemas: vec![(schema, 2)],
                ..Default::default()
            },
            ..Default::default()
        }
    };
    let error = block_on(db.apply_catalogue_keeping(other, &kept(b"b")))
        .expect_err("a different value for a kept key");
    assert_eq!(error.code, jazz::db::ErrorCode::Schema, "{error}");
    assert_eq!(db.host_get(KEPT, b"k").unwrap(), Some(b"a".to_vec()));
    assert_eq!(
        db.catalogue().unwrap().schemas.len(),
        1,
        "the change's schema was not published"
    );
}
