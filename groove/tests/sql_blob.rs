//! Tests for BLOB and BLOB[] column types.

use groove::sql::{decode_row, encode_row, ColumnDef, ColumnType, TableSchema, Value};
use groove::{ChunkHash, ContentRef};

fn blob_schema() -> TableSchema {
    TableSchema::new(
        "files",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("content", ColumnType::Blob),
        ],
    )
}

fn blob_array_schema() -> TableSchema {
    TableSchema::new(
        "documents",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::required("attachments", ColumnType::BlobArray),
        ],
    )
}

#[test]
fn encode_decode_blob_inline() {
    let schema = blob_schema();

    // Create a small inline blob
    let content = ContentRef::inline(vec![1, 2, 3, 4, 5]);
    let values = vec![Value::String("test.txt".into()), Value::Blob(content)];

    let encoded = encode_row(&values, &schema).expect("encode should succeed");
    let decoded = decode_row(&encoded, &schema).expect("decode should succeed");

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].as_str(), Some("test.txt"));

    let blob = decoded[1].as_blob().expect("should be blob");
    assert!(blob.is_inline());
    assert_eq!(blob.as_inline(), Some(&[1, 2, 3, 4, 5][..]));
}

#[test]
fn encode_decode_blob_chunked() {
    let schema = blob_schema();

    // Create a chunked blob with fake hashes
    let hashes = vec![
        ChunkHash::from_bytes([1u8; 32]),
        ChunkHash::from_bytes([2u8; 32]),
    ];
    let content = ContentRef::chunked(hashes);
    let values = vec![Value::String("large.bin".into()), Value::Blob(content)];

    let encoded = encode_row(&values, &schema).expect("encode should succeed");
    let decoded = decode_row(&encoded, &schema).expect("decode should succeed");

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].as_str(), Some("large.bin"));

    let blob = decoded[1].as_blob().expect("should be blob");
    assert!(!blob.is_inline());
    let chunks = blob.as_chunks().expect("should have chunks");
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].as_bytes(), &[1u8; 32]);
    assert_eq!(chunks[1].as_bytes(), &[2u8; 32]);
}

#[test]
fn encode_decode_blob_array() {
    let schema = blob_array_schema();

    // Create a blob array with mixed inline blobs
    let refs = vec![
        ContentRef::inline(vec![10, 20, 30]),
        ContentRef::inline(vec![40, 50]),
        ContentRef::chunked(vec![ChunkHash::from_bytes([99u8; 32])]),
    ];
    let values = vec![Value::String("report.doc".into()), Value::BlobArray(refs)];

    let encoded = encode_row(&values, &schema).expect("encode should succeed");
    let decoded = decode_row(&encoded, &schema).expect("decode should succeed");

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].as_str(), Some("report.doc"));

    let blobs = decoded[1].as_blob_array().expect("should be blob array");
    assert_eq!(blobs.len(), 3);

    assert!(blobs[0].is_inline());
    assert_eq!(blobs[0].as_inline(), Some(&[10, 20, 30][..]));

    assert!(blobs[1].is_inline());
    assert_eq!(blobs[1].as_inline(), Some(&[40, 50][..]));

    assert!(!blobs[2].is_inline());
    assert_eq!(blobs[2].as_chunks().unwrap().len(), 1);
}

#[test]
fn encode_decode_nullable_blob() {
    let schema = TableSchema::new(
        "files",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("content", ColumnType::Blob),
        ],
    );

    // Null blob
    let values = vec![
        Value::String("empty.txt".into()),
        Value::NullableNone,
    ];

    let encoded = encode_row(&values, &schema).expect("encode should succeed");
    let decoded = decode_row(&encoded, &schema).expect("decode should succeed");

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].as_str(), Some("empty.txt"));
    assert!(decoded[1].is_null());
}

#[test]
fn content_ref_roundtrip() {
    // Test inline roundtrip
    let inline = ContentRef::inline(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let bytes = inline.to_row_bytes();
    let (decoded, consumed) = ContentRef::from_row_bytes(&bytes).expect("decode should succeed");
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded, inline);

    // Test chunked roundtrip
    let hashes = vec![
        ChunkHash::from_bytes([1u8; 32]),
        ChunkHash::from_bytes([2u8; 32]),
        ChunkHash::from_bytes([3u8; 32]),
    ];
    let chunked = ContentRef::chunked(hashes);
    let bytes = chunked.to_row_bytes();
    let (decoded, consumed) = ContentRef::from_row_bytes(&bytes).expect("decode should succeed");
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded, chunked);
}

#[test]
fn blob_column_type_serialization() {
    let schema = TableSchema::new(
        "test",
        vec![
            ColumnDef::required("data", ColumnType::Blob),
            ColumnDef::required("attachments", ColumnType::BlobArray),
        ],
    );

    let bytes = schema.to_bytes();
    let decoded = TableSchema::from_bytes(&bytes).expect("decode should succeed");

    assert_eq!(decoded.columns.len(), 2);
    assert_eq!(decoded.columns[0].ty, ColumnType::Blob);
    assert_eq!(decoded.columns[1].ty, ColumnType::BlobArray);
}
