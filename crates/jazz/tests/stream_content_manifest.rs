use std::collections::BTreeMap;
use std::sync::Arc;

use jazz::content_manifest::{
    ContentDomainId, ContentManifest, ContentManifestRuntimeProvider, ContentManifestSchema,
    ContentReadContext, ImmutableContentStore, MaterializationRequest, MemoryImmutableContentStore,
};
use jazz::groove::records::Value;
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::schema::{ColumnSchema, JazzSchema, TableSchema};
use jazz::stream_manifest::StreamManifestAdapter;

struct Provider {
    domain: ContentDomainId,
    store: MemoryImmutableContentStore,
}

impl ContentManifestRuntimeProvider for Provider {
    fn read_context(&self, _: NodeUuid) -> ContentReadContext {
        ContentReadContext {
            domain: self.domain,
        }
    }

    fn immutable_store(&self) -> &dyn ImmutableContentStore {
        &self.store
    }
}

fn cell(manifest: &ContentManifest, schema: &ContentManifestSchema) -> Value {
    Value::Bytes(manifest.encode(schema).unwrap())
}

/// Black-boxes the public schema and Node seams. Adapter codec tests remain
/// internal, but admission, merge, materialization, range reads, historical
/// manifests, and interior index values must all traverse the production
/// registry/provider path here.
#[test]
fn registered_stream_manifest_runs_through_a_real_node() {
    let node_uuid = NodeUuid::from_bytes([0x51; 16]);
    let domain = ContentDomainId(node_uuid.0);
    let context = ContentReadContext { domain };
    let adapter = StreamManifestAdapter::new(4, 4).unwrap();
    let mut store = MemoryImmutableContentStore::default();

    let empty = adapter.empty_manifest(context, &mut store).unwrap();
    let immutable_history = adapter
        .append(&empty, b"abcde", context, &mut store)
        .unwrap();
    let current = adapter
        .append(&immutable_history, b"fg", context, &mut store)
        .unwrap();

    // The schema bound is deliberately wider than stream-v1's production
    // bound so row admission proves the registered adapter also validates it.
    let manifest_schema = ContentManifestSchema::new("stream-v1", 1, 512).unwrap();
    let schema = JazzSchema::new([TableSchema::new(
        "documents",
        [ColumnSchema::content_manifest(
            "attachment",
            manifest_schema.clone(),
        )],
    )]);
    let dir = tempfile::tempdir().unwrap();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let mut node = NodeState::new_with_content_manifest_provider(
        node_uuid,
        schema,
        storage,
        Arc::new(Provider { domain, store }),
        true,
    )
    .unwrap();
    let row = RowUuid::from_bytes([0x52; 16]);

    let base = node
        .commit_mergeable(
            MergeableCommit::new("documents", row, 10).cells(BTreeMap::from([(
                "attachment".into(),
                cell(&immutable_history, &manifest_schema),
            )])),
        )
        .unwrap();
    node.finalize_local_mergeable_commit(base).unwrap();

    // Two concurrent heads carrying the same complete snapshot force the
    // production manifest merge path without inventing ordering for distinct
    // concurrent stream appends, which stream-v1 deliberately rejects.
    let left = node
        .commit_mergeable(
            MergeableCommit::new("documents", row, 20)
                .parents(vec![base])
                .cells(BTreeMap::from([(
                    "attachment".into(),
                    cell(&current, &manifest_schema),
                )])),
        )
        .unwrap();
    node.finalize_local_mergeable_commit(left).unwrap();
    let right = node
        .commit_mergeable(
            MergeableCommit::new("documents", row, 21)
                .parents(vec![base])
                .cells(BTreeMap::from([(
                    "attachment".into(),
                    cell(&current, &manifest_schema),
                )])),
        )
        .unwrap();
    node.finalize_local_mergeable_commit(right).unwrap();

    let visible = node
        .visible_current_cells("documents", row)
        .unwrap()
        .unwrap();
    let visible_cell = &visible["attachment"];
    assert_eq!(
        node.materialize_content_manifest(
            "documents",
            "attachment",
            visible_cell,
            &MaterializationRequest::Full,
        )
        .unwrap(),
        b"abcdefg"
    );
    assert_eq!(
        node.materialize_content_manifest(
            "documents",
            "attachment",
            visible_cell,
            &MaterializationRequest::Range {
                offset: 3,
                length: 4,
            },
        )
        .unwrap(),
        b"defg"
    );
    assert_eq!(
        node.materialize_content_manifest(
            "documents",
            "attachment",
            &cell(&immutable_history, &manifest_schema),
            &MaterializationRequest::Full,
        )
        .unwrap(),
        b"abcde",
        "a direct historical manifest must not consult current row history"
    );
    let index = node
        .content_manifest_index_values("documents", "attachment", visible_cell, &["length".into()])
        .unwrap();
    assert_eq!(
        u64::from_le_bytes(index["length"].as_slice().try_into().unwrap()),
        7
    );

    let invalid = ContentManifest {
        root: current.root,
        edit_tail: vec![vec![0; 257]],
    };
    assert!(
        node.commit_mergeable(
            MergeableCommit::new("documents", RowUuid::from_bytes([0x53; 16]), 30).cells(
                BTreeMap::from([("attachment".into(), cell(&invalid, &manifest_schema))]),
            ),
        )
        .is_err(),
        "Node row admission must call stream-v1 operation validation"
    );
}
