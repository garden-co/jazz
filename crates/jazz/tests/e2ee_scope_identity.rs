mod common;

use futures::executor::block_on;
use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::CurrentWriteSchema;
use jazz::protocol::{LensOp, MigrationLens, SchemaVersion, TableLens};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

fn schema(table: &str) -> JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column("title", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    )
}

#[test]
fn scope_identity_is_available_only_after_catalogue_acceptance_and_survives_rename() {
    block_on(async {
        let base = schema("projects");
        let base_id = base.version_id();
        let cfs = base.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = Db::open(DbConfig::new(
            base,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x61; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ))
        .await
        .unwrap();
        let initial = db
            .catalogue_table_identity(base_id, "projects")
            .unwrap()
            .unwrap();
        assert!(!initial.0.is_nil());
        assert_eq!(
            db.catalogue_table_identity(base_id, "missing").unwrap(),
            None
        );

        let renamed = SchemaVersion::new(schema("workspaces"));
        let publication = db
            .author_schema_lineage_publication(
                renamed.clone(),
                MigrationLens::new(
                    base_id,
                    renamed.id,
                    vec![TableLens {
                        source_table: "projects".to_owned(),
                        target_table: "workspaces".to_owned(),
                        ops: vec![LensOp::RenameTable {
                            from: "projects".to_owned(),
                            to: "workspaces".to_owned(),
                        }],
                    }],
                )
                .unwrap(),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap();
        assert_eq!(
            publication.physical_identities.tables["workspaces"].id,
            initial
        );
        assert_eq!(
            db.catalogue_table_identity(renamed.id, "workspaces")
                .unwrap(),
            None
        );
        db.publish_schema_with_lens(1, publication).await.unwrap();
        assert_eq!(
            db.catalogue_table_identity(renamed.id, "workspaces")
                .unwrap(),
            Some(initial)
        );
        assert_eq!(
            db.catalogue_table_identity(base_id, "projects").unwrap(),
            Some(initial)
        );
        assert_eq!(
            db.catalogue_table_identity(renamed.id, "projects").unwrap(),
            None
        );
        let old_view = db.register_schema_view(schema("projects")).await.unwrap();
        db.set_current_write_schema(CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        })
        .await
        .unwrap();
        assert_eq!(
            db.table_identity("workspaces").await.unwrap(),
            Some(initial)
        );
        assert_eq!(db.table_identity("projects").await.unwrap(), None);
        assert_eq!(
            old_view.table_identity("projects").await.unwrap(),
            Some(initial)
        );
        assert_eq!(old_view.table_identity("workspaces").await.unwrap(), None);
    });
}
