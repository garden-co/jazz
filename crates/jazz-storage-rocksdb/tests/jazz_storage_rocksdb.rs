use groove::storage::{LayoutStorage, OrderedKvStorage, StorageLayout};
use jazz_storage_rocksdb::RocksDbStorage;

#[futures_test::test]
async fn class_layout_v1_writes_exact_rocks_marker_and_mapped_key_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let logical_cf = "jazz_albums_history";
    let physical_cfs = StorageLayout::jazz_class_v1().physical_column_families([logical_cf]);
    let refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let layout = LayoutStorage::new(
        RocksDbStorage::open(directory.path(), &refs).unwrap(),
        StorageLayout::jazz_class_v1(),
    )
    .await
    .unwrap();
    layout
        .set(logical_cf.into(), b"row\0key".to_vec(), b"value".to_vec())
        .await
        .unwrap();

    // Inspect the physical adapter directly so a matching change in both
    // mapping and logical reads cannot hide a durable-byte drift.
    let raw = layout.into_inner();
    assert_eq!(
        raw.get(
            "__groove_class_meta".into(),
            b"groove-storage-layout".to_vec()
        )
        .await
        .unwrap(),
        Some(b"class-cf-v1".to_vec())
    );
    let mut expected_key = (logical_cf.len() as u32).to_be_bytes().to_vec();
    expected_key.extend_from_slice(logical_cf.as_bytes());
    expected_key.extend_from_slice(b"row\0key");
    assert_eq!(
        raw.get("__groove_class_history".into(), expected_key)
            .await
            .unwrap(),
        Some(b"value".to_vec())
    );
}
