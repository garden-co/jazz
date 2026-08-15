//! Black-box receipt for the production `json-v1` manifest registration and
//! node/runtime bridge. All schema and node interactions use public APIs.

use std::collections::BTreeMap;
use std::sync::Arc;

use jazz::content_manifest::{
    ContentDomainId, ContentManifest, ContentManifestRuntime, ContentManifestRuntimeProvider,
    ContentManifestSchema, ContentReadContext, ImmutableContentStore, MaterializationRequest,
    MemoryImmutableContentStore, global_content_manifest_adapters,
};
use jazz::groove::records::{Value, ValueType};
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::ordinary_json::{JsonLiteral, JsonScalar, OrdinaryJsonAdapter};
use jazz::schema::{ColumnSchema, JazzSchema, TableSchema};
use uuid::Uuid;

struct Provider {
    context: ContentReadContext,
    store: MemoryImmutableContentStore,
}

impl ContentManifestRuntimeProvider for Provider {
    fn read_context(&self, _: NodeUuid) -> ContentReadContext {
        self.context
    }
    fn immutable_store(&self) -> &dyn ImmutableContentStore {
        &self.store
    }
}

#[test]
fn json_manifest_runs_through_schema_admission_node_and_projection_runtime() {
    let adapter = OrdinaryJsonAdapter;
    let context = ContentReadContext {
        domain: ContentDomainId(Uuid::from_bytes([0x61; 16])),
    };
    let mut store = MemoryImmutableContentStore::default();
    let root = adapter
        .publish_literal(
            &JsonLiteral::Object(BTreeMap::from([
                (
                    "status".into(),
                    JsonLiteral::Scalar(JsonScalar::String("open".into())),
                ),
                (
                    "items".into(),
                    JsonLiteral::Array(vec![
                        JsonLiteral::Scalar(JsonScalar::Number(1)),
                        JsonLiteral::Scalar(JsonScalar::Number(3)),
                    ]),
                ),
            ])),
            None,
            context,
            &mut store,
        )
        .unwrap();
    let base = ContentManifest {
        root,
        edit_tail: Vec::new(),
    };
    let left_op = adapter
        .author_insert_at_index(
            &base,
            "/items",
            1,
            Uuid::from_bytes([0x62; 16]),
            Uuid::from_bytes([0x63; 16]),
            JsonLiteral::Scalar(JsonScalar::Number(2)),
            context,
            &store,
        )
        .unwrap()
        .encode()
        .unwrap();
    let right_op = adapter
        .author_insert_at_index(
            &base,
            "/items",
            2,
            Uuid::from_bytes([0x64; 16]),
            Uuid::from_bytes([0x65; 16]),
            JsonLiteral::Scalar(JsonScalar::Number(4)),
            context,
            &store,
        )
        .unwrap()
        .encode()
        .unwrap();

    let manifest_schema =
        ContentManifestSchema::with_tail_entry_type("json-v1", ValueType::Bytes, 8, 4096).unwrap();
    let schema = JazzSchema::new([TableSchema::new(
        "documents",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::content_manifest("body", manifest_schema.clone()),
        ],
    )]);
    let refs = schema.column_families();
    let storage = MemoryStorage::new(&refs.iter().map(String::as_str).collect::<Vec<_>>());
    let provider = Arc::new(Provider { context, store });
    let mut node = NodeState::new_with_content_manifest_provider(
        NodeUuid::from_bytes([0x66; 16]),
        schema,
        storage,
        provider.clone(),
        false,
    )
    .unwrap();

    let left = ContentManifest {
        root,
        edit_tail: vec![left_op],
    };
    let left_cell = left.into_value(&manifest_schema).unwrap();
    let row = RowUuid::from_bytes([0x67; 16]);
    node.commit_mergeable(
        MergeableCommit::new("documents", row, 1).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("example".to_owned())),
            ("body".to_owned(), left_cell.clone()),
        ])),
    )
    .unwrap();
    let visible = node
        .visible_current_cells("documents", row)
        .unwrap()
        .unwrap();
    assert_eq!(visible["body"], left_cell);

    assert_eq!(
        node.materialize_content_manifest(
            "documents",
            "body",
            &visible["body"],
            &MaterializationRequest::Projection(vec!["/items".into()]),
        )
        .unwrap(),
        br#"{"/items":[1,2,3]}"#
    );
    assert_eq!(
        node.content_manifest_index_values(
            "documents",
            "body",
            &visible["body"],
            &["/items/1".into()],
        )
        .unwrap()["/items/1"],
        b"2"
    );

    let runtime = ContentManifestRuntime::new(
        global_content_manifest_adapters(),
        context,
        provider.immutable_store(),
    );
    let right = ContentManifest {
        root,
        edit_tail: vec![right_op],
    };
    let merged = runtime
        .merge_cells(
            &manifest_schema,
            &[
                visible["body"].clone(),
                right.into_value(&manifest_schema).unwrap(),
            ],
        )
        .unwrap();
    assert_eq!(
        runtime
            .materialize_cell(&manifest_schema, &merged, &MaterializationRequest::Full)
            .unwrap(),
        br#"{"items":[1,2,3,4],"status":"open"}"#
    );

    let bundle = adapter
        .eventual_projection_bundle(
            &base,
            &["/status".into()],
            context,
            provider.immutable_store(),
        )
        .unwrap();
    let merged_manifest = ContentManifest::from_value(&merged, &manifest_schema).unwrap();
    assert!(
        adapter
            .checked_eventual_projection(&bundle, &merged_manifest)
            .is_err()
    );
}
