#![cfg(feature = "native")]

use jazz::catalogue::CatalogueChange;
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::host::{HostTable, HostTables, StoreFile};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::storage::Storage;
use jazz::tools::public_schema::{ColumnType, SchemaBuilder, TableSchema};
use jazz::tools::test_support::AllowAll;
use jazz_server::runtime::catalogue_schema;

fn config(schema: JazzSchema) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([19; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// Alice's host reads its catalogue before opening the Db, then reuses that
/// storage owner. A restart recovers the activated permissions and old bundle.
#[test]
fn owned_storage_recovers_catalogue_before_database_open() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority.sqlite");
        let source = SchemaBuilder::new()
            .table(TableSchema::builder("docs").column("title", ColumnType::Text))
            .build();
        let denied = JazzSchema::new(&source).unwrap();
        let allowed = source.clone().allow_all();
        let store = StoreFile::from_storage(Storage::open(&path).unwrap()).unwrap();
        assert!(
            catalogue_schema(&store.catalogue().unwrap())
                .unwrap()
                .is_none()
        );
        let table = HostTable::new("startup_probe");
        store
            .host_put_many(table, &[(b"alice".to_vec(), b"kept".to_vec())])
            .unwrap();
        let db = Db::open_file(config(JazzSchema::new(&allowed).unwrap()), store)
            .await
            .unwrap();
        db.set_authority();
        let bundle = db.publish_bundle(&allowed).await.unwrap();
        let genesis = catalogue_schema(&db.catalogue().unwrap()).unwrap().unwrap();
        assert_eq!(genesis.version_id(), denied.version_id());
        db.apply_catalogue(CatalogueChange {
            activate: Some((1, denied.clone(), Vec::new())),
            ..Default::default()
        })
        .await
        .unwrap();
        let before = db.catalogue().unwrap();
        drop(db);

        let store = StoreFile::from_storage(Storage::open(&path).unwrap()).unwrap();
        let rows = store.catalogue().unwrap();
        assert_eq!(rows, before);
        let schema = catalogue_schema(&rows).unwrap().unwrap();
        assert_eq!(schema.public_schema(), denied.public_schema());
        assert_eq!(
            store.host_get(table, b"alice").unwrap(),
            Some(b"kept".to_vec())
        );
        let db = Db::open_file(config(schema), store).await.unwrap();
        db.set_authority();
        assert_eq!(
            db.catalogue().unwrap(),
            before,
            "reopening does not republish"
        );
        assert!(
            db.pinned_view(denied.version_id(), Some(&bundle))
                .unwrap()
                .is_some()
        );
        assert_eq!(
            db.host_get(table, b"alice").unwrap(),
            Some(b"kept".to_vec())
        );
    });
}

/// Alice's host refuses a broken current revision rather than treating the
/// store as new or falling back to an older admitted schema.
#[test]
fn missing_current_schema_is_not_a_fresh_store() {
    let store = StoreFile::from_storage(Storage::open_in_memory().unwrap()).unwrap();
    store
        .catalogue_put(&jazz::catalogue::CataloguePut {
            permissions: vec![(
                1,
                jazz::tools::public_schema::SchemaHash::from_bytes([7; 32]),
                Default::default(),
                1,
            )],
            ..Default::default()
        })
        .unwrap();
    assert!(
        catalogue_schema(&store.catalogue().unwrap())
            .err()
            .unwrap()
            .contains("schema is missing")
    );
}
