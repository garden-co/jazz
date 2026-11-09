#![cfg(feature = "native")]

use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::host::StoreFile;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::storage::Storage;
use jazz::tools::public_schema::{ColumnType, Schema, SchemaBuilder, SchemaHash, TableSchema};
use jazz_server::runtime::ServerCore;
use jazz_server::runtime::deployment::{DeployRequest, DeploySchema};

fn request(schema: &Schema) -> DeployRequest {
    let hash = SchemaHash::compute(schema).to_string();
    DeployRequest {
        target_schema_hash: hash.clone(),
        schemas: vec![DeploySchema {
            hash,
            schema: schema.clone(),
        }],
        migrations: vec![],
        permissions: Default::default(),
        allow_dropping_history: false,
    }
}

/// Alice's backend publishes through the shared owner, retries without rewriting,
/// and recovers the same bundle after restart. Mallory's mismatched schema hash
/// must leave the published catalogue unchanged.
#[test]
fn owner_publication_is_idempotent_and_persistent() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("authority.sqlite");
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("notes").column("title", ColumnType::Text))
            .build();
        let config = || {
            DbConfig::new(
                JazzSchema::new(&schema).unwrap(),
                DbIdentity {
                    node: NodeUuid::from_bytes([31; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            )
        };
        let open = || StoreFile::from_storage(Storage::open(&path).unwrap()).unwrap();
        let db = Db::open_file(config(), open()).await.unwrap();
        let mut core = ServerCore::from_db(db, None, std::task::Waker::noop().clone()).unwrap();
        let response = core
            .deploy(request(&schema), 100)
            .await
            .unwrap_or_else(|err| panic!("{}", err.body));
        assert!(response.changed);
        let before = serde_json::to_value(core.catalogue_snapshot().unwrap().bundles()).unwrap();
        let retry = core
            .deploy(request(&schema), 200)
            .await
            .unwrap_or_else(|err| panic!("{}", err.body));
        assert!(!retry.changed);
        assert_eq!(retry.bundle, response.bundle);
        let mut invalid = request(&schema);
        invalid.schemas[0].hash = SchemaHash::compute(&Schema::default()).to_string();
        let failure = core.deploy(invalid, 300).await.err().unwrap();
        assert_eq!(failure.status.as_u16(), 422);
        assert_eq!(failure.body["code"], "schema_hash_mismatch");
        assert_eq!(
            serde_json::to_value(core.catalogue_snapshot().unwrap().bundles()).unwrap(),
            before
        );
        drop(core);
        let db = Db::open_file(config(), open()).await.unwrap();
        let core = ServerCore::from_db(db, None, std::task::Waker::noop().clone()).unwrap();
        assert_eq!(
            serde_json::to_value(core.catalogue_snapshot().unwrap().bundles()).unwrap(),
            before
        );
        let published = serde_json::to_value(
            core.catalogue_snapshot()
                .unwrap()
                .schema(&SchemaHash::compute(&schema))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(published["publishedAt"], 100);
    });
}
