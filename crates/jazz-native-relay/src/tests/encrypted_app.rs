//! A relay serves a client's app on its persistent store, and a client has
//! no encryption hooks yet: a relay refuses to open an app whose schema
//! holds Jazz's system tables, before it creates the store.
use super::*;
use jazz::db::Migrations;
use jazz::ids::AuthorSubject;
use jazz::tools::deployment::artifact::system_tables::with_e2ee_fragment;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

fn config(schema: jazz::schema::JazzSchema, path: PathBuf) -> RelayOpenConfig {
    RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "app".to_owned(),
            storage_namespace: "default".to_owned(),
            auth_scope: Some("alice".to_owned()),
        },
        sqlite_path: path,
        schema,
        migrations: Migrations::default(),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x01; 16]),
            author: AuthorSubject::for_test_bytes([0x02; 16]),
        },
    }
}

/// ```text
/// relay ◄── encrypted app's schema ──✗ refused, no store
/// relay ◄── ordinary schema ─────────✓
/// ```
#[test]
fn a_relay_refuses_an_app_that_declares_encryption() {
    let directory = tempfile::tempdir().unwrap();
    let app = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("notes").column("title", ColumnType::Text))
        .build();
    let encrypted =
        jazz::schema::JazzSchema::new(&with_e2ee_fragment(&app)).expect("a compiled schema");
    let path = directory.path().join("encrypted.sqlite");
    match NativeRelay::spawn(config(encrypted, path.clone())) {
        Err(RelayError::Host(error)) => {
            let message = error.to_string();
            assert!(
                message.contains("declares end-to-end encryption"),
                "{message}"
            );
        }
        Err(error) => panic!("expected a refusal, got {error}"),
        Ok(_) => panic!("a relay opened an encrypted app"),
    }
    let ordinary = jazz::schema::JazzSchema::new(&app).unwrap();
    NativeRelay::spawn(config(ordinary, directory.path().join("ordinary.sqlite")))
        .expect("an ordinary app opens");
}
