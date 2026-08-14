#![cfg(all(feature = "test-utils", feature = "rocksdb"))]

//! Compressed RocksDB storage receipt for three ordinary-row JSON shapes.
//!
//! Run explicitly; this is a design measurement, not a latency gate:
//!
//! `JAZZ_JSON_STORAGE_DOCS=50 JAZZ_JSON_STORAGE_EDITS=10 cargo test -p jazz --features test,rocksdb --test ordinary_json_storage_receipt -- --ignored --nocapture`

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use jazz::row_input;
use jazz::tools::{
    AppContext, AppId, ClientStorage, ColumnType, JazzClient, JsonDocumentSchema,
    JsonDocumentStore, Schema, TableName, TableSchema, Value,
};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;

const WHOLE_TABLE: &str = "whole_json_rows";
const MUTABLE_DOCUMENTS: &str = "mutable_json_documents";
const MUTABLE_PARTS: &str = "mutable_json_parts";
const MUTABLE_PATHS: &str = "mutable_json_paths";

#[derive(Clone, Copy, Debug, Default)]
struct DiskBytes {
    apparent: u64,
    allocated: u64,
}

impl DiskBytes {
    fn subtract(self, baseline: Self) -> Self {
        Self {
            apparent: self.apparent.saturating_sub(baseline.apparent),
            allocated: self.allocated.saturating_sub(baseline.allocated),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct StorageReceipt {
    seeded: DiskBytes,
    edited: DiskBytes,
    edited_closed: DiskBytes,
}

fn comparison_schema() -> (Schema, JsonDocumentSchema) {
    let documents = JsonDocumentSchema::new("persistent_json")
        .project("/metadata/status")
        .expect("status pointer")
        .project("/metadata/priority")
        .expect("priority pointer");
    let mut schema = Schema::new();
    documents
        .install(&mut schema)
        .expect("install persistent JSON tables");
    for builder in [
        TableSchema::builder(WHOLE_TABLE).column("body", ColumnType::Json { schema: None }),
        TableSchema::builder(MUTABLE_DOCUMENTS).column("revision", ColumnType::BigInt),
        TableSchema::builder(MUTABLE_PARTS)
            .column("document_id", ColumnType::Uuid)
            .column("pointer", ColumnType::Text)
            .column("scalar_json", ColumnType::Text),
        TableSchema::builder(MUTABLE_PATHS)
            .column("document_id", ColumnType::Uuid)
            .column("pointer", ColumnType::Text)
            .column("scalar_json", ColumnType::Text)
            .index_only(["document_id", "pointer", "scalar_json"]),
    ] {
        let (name, table) = builder.build_named();
        assert!(
            schema.insert(name, table).is_none(),
            "unique benchmark table"
        );
    }
    (schema, documents)
}

async fn connect(dir: &Path, schema: Schema, label: &str) -> JazzClient {
    JazzClient::connect(AppContext {
        app_id: AppId::from_name(&format!("ordinary-json-storage-{label}")),
        client_id: None,
        schema,
        server_url: String::new(),
        data_dir: dir.to_path_buf(),
        storage: ClientStorage::Persistent,
        jwt_token: None,
        backend_secret: None,
        admin_secret: None,
    })
    .await
    .expect("open isolated RocksDB client")
}

fn document(bytes: usize, status: &str, seed: usize) -> JsonValue {
    let leaf_bytes = bytes.saturating_sub(256) / 32;
    let payload: Vec<_> = (0..32)
        .map(|leaf| {
            (0..leaf_bytes)
                .map(|offset| char::from(b'a' + ((offset + leaf * 7 + seed * 11) % 26) as u8))
                .collect::<String>()
        })
        .collect();
    json!({
        "metadata": {"status": status, "priority": seed as i64 % 5},
        "payload": payload,
    })
}

fn scalar_parts(value: &JsonValue) -> Vec<(String, String)> {
    let status = serde_json::to_string(&value["metadata"]["status"]).expect("status JSON");
    let priority = serde_json::to_string(&value["metadata"]["priority"]).expect("priority JSON");
    let mut parts = vec![
        ("/metadata/status".to_owned(), status),
        ("/metadata/priority".to_owned(), priority),
    ];
    for (index, leaf) in value["payload"]
        .as_array()
        .expect("payload array")
        .iter()
        .enumerate()
    {
        parts.push((
            format!("/payload/{index}"),
            serde_json::to_string(leaf).expect("leaf JSON"),
        ));
    }
    parts
}

fn directory_bytes(path: &Path) -> DiskBytes {
    fn visit(path: &Path, total: &mut DiskBytes) {
        let metadata = fs::symlink_metadata(path).expect("stat RocksDB path");
        if metadata.is_file() {
            total.apparent += metadata.len();
            #[cfg(unix)]
            {
                total.allocated += metadata.blocks() * 512;
            }
            #[cfg(not(unix))]
            {
                total.allocated += metadata.len();
            }
        } else if metadata.is_dir() {
            for entry in fs::read_dir(path).expect("read RocksDB directory") {
                visit(&entry.expect("directory entry").path(), total);
            }
        }
    }

    let mut total = DiskBytes::default();
    visit(path, &mut total);
    total
}

async fn baseline_bytes(schema: &Schema) -> (TempDir, DiskBytes, DiskBytes) {
    let dir = TempDir::new().expect("baseline tempdir");
    let client = connect(dir.path(), schema.clone(), "baseline").await;
    let open = directory_bytes(dir.path());
    client.shutdown().await.expect("close baseline client");
    let closed = directory_bytes(dir.path());
    (dir, open, closed)
}

async fn whole_json_receipt(
    schema: &Schema,
    open_baseline: DiskBytes,
    closed_baseline: DiskBytes,
    docs: usize,
    edits: usize,
    bytes: usize,
) -> StorageReceipt {
    let dir = TempDir::new().expect("whole JSON tempdir");
    let client = connect(dir.path(), schema.clone(), "whole").await;
    let mut ids = Vec::with_capacity(docs);
    for index in 0..docs {
        let value = serde_json::to_string(&document(bytes, "open", index)).expect("whole JSON");
        ids.push(
            client
                .insert(WHOLE_TABLE, row_input!("body" => Value::Text(value)))
                .expect("insert whole JSON")
                .0,
        );
    }
    let seeded = directory_bytes(dir.path()).subtract(open_baseline);
    for edit in 0..edits {
        let status = if edit % 2 == 0 { "closed" } else { "open" };
        for (index, id) in ids.iter().enumerate() {
            let value = serde_json::to_string(&document(bytes, status, index)).expect("whole JSON");
            client
                .update(*id, vec![("body".to_owned(), Value::Text(value))])
                .expect("rewrite whole JSON");
        }
    }
    let edited = directory_bytes(dir.path()).subtract(open_baseline);
    client.shutdown().await.expect("close edited whole client");
    StorageReceipt {
        seeded,
        edited,
        edited_closed: directory_bytes(dir.path()).subtract(closed_baseline),
    }
}

async fn mutable_parts_receipt(
    schema: &Schema,
    open_baseline: DiskBytes,
    closed_baseline: DiskBytes,
    docs: usize,
    edits: usize,
    bytes: usize,
) -> StorageReceipt {
    let dir = TempDir::new().expect("mutable parts tempdir");
    let client = connect(dir.path(), schema.clone(), "mutable").await;
    let mut rows = Vec::with_capacity(docs);
    for index in 0..docs {
        let value = document(bytes, "open", index);
        let transaction = client.begin_transaction().expect("begin mutable create");
        let (document_id, _, _) = transaction
            .insert(
                MUTABLE_DOCUMENTS,
                row_input!("revision" => Value::BigInt(0)),
            )
            .expect("insert mutable document");
        let mut status_part = None;
        for (pointer, scalar) in scalar_parts(&value) {
            let (part_id, _, _) = transaction
                .insert(
                    MUTABLE_PARTS,
                    row_input!(
                        "document_id" => document_id,
                        "pointer" => pointer.clone(),
                        "scalar_json" => scalar.clone()
                    ),
                )
                .expect("insert mutable part");
            if pointer == "/metadata/status" {
                status_part = Some(part_id);
            }
        }
        let (projection_id, _, _) = transaction
            .insert(
                MUTABLE_PATHS,
                row_input!(
                    "document_id" => document_id,
                    "pointer" => "/metadata/status",
                    "scalar_json" => "\"open\""
                ),
            )
            .expect("insert mutable projection");
        transaction
            .insert(
                MUTABLE_PATHS,
                row_input!(
                    "document_id" => document_id,
                    "pointer" => "/metadata/priority",
                    "scalar_json" => serde_json::to_string(&value["metadata"]["priority"])
                        .expect("priority JSON")
                ),
            )
            .expect("insert mutable priority projection");
        transaction.commit().expect("commit mutable create");
        rows.push((
            document_id,
            status_part.expect("status part"),
            projection_id,
        ));
    }
    let seeded = directory_bytes(dir.path()).subtract(open_baseline);
    for edit in 0..edits {
        let status = if edit % 2 == 0 {
            "\"closed\""
        } else {
            "\"open\""
        };
        for (document_id, part_id, projection_id) in &rows {
            let transaction = client.begin_transaction().expect("begin mutable edit");
            transaction
                .update(
                    *document_id,
                    vec![("revision".to_owned(), Value::BigInt((edit + 1) as i64))],
                )
                .expect("version mutable document");
            for id in [part_id, projection_id] {
                transaction
                    .update(
                        *id,
                        vec![("scalar_json".to_owned(), Value::Text(status.to_owned()))],
                    )
                    .expect("update mutable scalar/projection");
            }
            transaction.commit().expect("commit mutable edit");
        }
    }
    let edited = directory_bytes(dir.path()).subtract(open_baseline);
    client
        .shutdown()
        .await
        .expect("close edited mutable client");
    StorageReceipt {
        seeded,
        edited,
        edited_closed: directory_bytes(dir.path()).subtract(closed_baseline),
    }
}

async fn persistent_root_receipt(
    schema: &Schema,
    documents: &JsonDocumentSchema,
    open_baseline: DiskBytes,
    closed_baseline: DiskBytes,
    docs: usize,
    edits: usize,
    bytes: usize,
) -> StorageReceipt {
    let dir = TempDir::new().expect("persistent root tempdir");
    let client = connect(dir.path(), schema.clone(), "persistent").await;
    let store = JsonDocumentStore::new(&client, documents);
    let mut ids = Vec::with_capacity(docs);
    for index in 0..docs {
        ids.push(
            store
                .create(&document(bytes, "open", index))
                .expect("create persistent document")
                .document_id,
        );
    }
    let seeded = directory_bytes(dir.path()).subtract(open_baseline);
    for edit in 0..edits {
        let status = if edit % 2 == 0 { "closed" } else { "open" };
        for id in &ids {
            store
                .set_scalar(*id, "/metadata/status", &json!(status))
                .await
                .expect("edit persistent scalar");
        }
    }
    let edited = directory_bytes(dir.path()).subtract(open_baseline);
    client.shutdown().await.expect("close persistent client");
    StorageReceipt {
        seeded,
        edited,
        edited_closed: directory_bytes(dir.path()).subtract(closed_baseline),
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "compressed-storage design receipt; invoke explicitly"]
async fn ordinary_json_compressed_rocksdb_storage_receipt() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let docs = std::env::var("JAZZ_JSON_STORAGE_DOCS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20usize);
            let edits = std::env::var("JAZZ_JSON_STORAGE_EDITS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10usize);
            let bytes = std::env::var("JAZZ_JSON_STORAGE_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10 * 1024usize);
            let (schema, documents) = comparison_schema();
            let (_baseline_dir, open_baseline, closed_baseline) = baseline_bytes(&schema).await;
            let whole = whole_json_receipt(
                &schema,
                open_baseline,
                closed_baseline,
                docs,
                edits,
                bytes,
            )
            .await;
            let mutable = mutable_parts_receipt(
                &schema,
                open_baseline,
                closed_baseline,
                docs,
                edits,
                bytes,
            )
            .await;
            let persistent = persistent_root_receipt(
                &schema,
                &documents,
                open_baseline,
                closed_baseline,
                docs,
                edits,
                bytes,
            )
            .await;

            eprintln!(
                "ORDINARY_JSON_ROCKSDB_RECEIPT docs={docs} edits_per_doc={edits} logical_bytes_per_doc={bytes} compression=history:zstd,current:lz4,bottommost:zstd baseline_open_apparent={} baseline_open_allocated={} baseline_closed_apparent={} baseline_closed_allocated={} whole_seed_open_apparent={} whole_seed_open_allocated={} whole_edited_open_apparent={} whole_edited_open_allocated={} whole_edited_closed_apparent={} whole_edited_closed_allocated={} mutable_seed_open_apparent={} mutable_seed_open_allocated={} mutable_edited_open_apparent={} mutable_edited_open_allocated={} mutable_edited_closed_apparent={} mutable_edited_closed_allocated={} persistent_seed_open_apparent={} persistent_seed_open_allocated={} persistent_edited_open_apparent={} persistent_edited_open_allocated={} persistent_edited_closed_apparent={} persistent_edited_closed_allocated={} measurement=open_wal_and_closed_db_no_manual_flush_or_compaction",
                open_baseline.apparent,
                open_baseline.allocated,
                closed_baseline.apparent,
                closed_baseline.allocated,
                whole.seeded.apparent,
                whole.seeded.allocated,
                whole.edited.apparent,
                whole.edited.allocated,
                whole.edited_closed.apparent,
                whole.edited_closed.allocated,
                mutable.seeded.apparent,
                mutable.seeded.allocated,
                mutable.edited.apparent,
                mutable.edited.allocated,
                mutable.edited_closed.apparent,
                mutable.edited_closed.allocated,
                persistent.seeded.apparent,
                persistent.seeded.allocated,
                persistent.edited.apparent,
                persistent.edited.allocated,
                persistent.edited_closed.apparent,
                persistent.edited_closed.allocated,
            );

            assert!(whole.edited.apparent >= whole.seeded.apparent);
            assert!(mutable.edited.apparent >= mutable.seeded.apparent);
            assert!(persistent.edited.apparent >= persistent.seeded.apparent);
        })
        .await;
}

#[test]
fn comparison_schema_is_identical_for_all_three_storage_scenarios() {
    let (schema, documents) = comparison_schema();
    for table in [
        WHOLE_TABLE,
        MUTABLE_DOCUMENTS,
        MUTABLE_PARTS,
        MUTABLE_PATHS,
        &documents.names.documents,
        &documents.names.roots,
        &documents.names.parts,
        &documents.names.projections,
    ] {
        assert!(
            schema.contains_key(&TableName::new(table)),
            "missing {table}"
        );
    }
}
