//! The metering adapter's physical table/index categories are internal. Public
//! queries cannot scan across mixed internal index families or malformed index
//! keys, so check those counters through the adapter's storage API here.

use super::*;
use crate::db::storage_helpers::MeteredStorage;
use crate::storage::{ScanRequest, collect_scan};

#[futures_test::test]
async fn table_scan_metrics_count_returned_batches_empty_scans_and_errors_exactly() {
    let storage = MemoryStorage::new(&["jazz_fixture_global_current"]).unwrap();
    for id in 0..600_u64 {
        storage
            .set(
                "jazz_fixture_global_current".into(),
                id.to_be_bytes().to_vec(),
                vec![1],
            )
            .await
            .unwrap();
    }
    let counters = RefCell::new(StorageReadMetrics::default());
    let metered = MeteredStorage::new(&storage, &counters);
    for limit in [0, 1, 257, 600, 700] {
        *counters.borrow_mut() = StorageReadMetrics::default();
        let rows = collect_scan(
            metered
                .scan(
                    ScanRequest::prefix("jazz_fixture_global_current".into(), vec![])
                        .reversed()
                        .with_max_items(limit),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), limit.min(600));
        assert!(rows.windows(2).all(|pair| pair[0].0 > pair[1].0));
        let expected = StorageReadBucket {
            reads: limit.min(600),
            ranges: 1,
        };
        assert_eq!(
            *counters.borrow(),
            StorageReadMetrics {
                total: expected,
                global_current_rows: expected,
                ..StorageReadMetrics::default()
            }
        );
    }

    *counters.borrow_mut() = StorageReadMetrics::default();
    let rows = collect_scan(
        metered
            .scan(ScanRequest::prefix(
                "jazz_fixture_global_current".into(),
                vec![255],
            ))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(rows.is_empty());
    assert_eq!(
        counters.borrow().total,
        StorageReadBucket {
            reads: 0,
            ranges: 1
        }
    );
    assert_eq!(
        counters.borrow().global_current_rows,
        counters.borrow().total
    );

    *counters.borrow_mut() = StorageReadMetrics::default();
    assert!(
        metered
            .scan(ScanRequest::prefix("jazz_missing_history".into(), vec![]))
            .await
            .is_err()
    );
    assert_eq!(
        *counters.borrow(),
        StorageReadMetrics {
            total: StorageReadBucket {
                reads: 0,
                ranges: 1
            },
            history_rows: StorageReadBucket {
                reads: 0,
                ranges: 1
            },
            ..StorageReadMetrics::default()
        }
    );
}

#[futures_test::test]
async fn mixed_index_scans_keep_each_keys_destination_and_the_requests_range_bucket() {
    let storage = MemoryStorage::new(&["indices"]).unwrap();
    let keys = [
        b"jazz_fixture_global_current\0by_app_owner\0one".to_vec(),
        b"jazz_fixture_global_current\0by_app_owner\0two".to_vec(),
        b"jazz_fixture_history\0by_tx\0one".to_vec(),
        b"jazz_transactions\0by_global_time\0one".to_vec(),
        b"unknown\0index\0one".to_vec(),
        b"malformed".to_vec(),
        vec![255, 0, 0],
    ];
    for key in &keys {
        storage
            .set("indices".into(), key.clone(), vec![1])
            .await
            .unwrap();
    }
    let counters = RefCell::new(StorageReadMetrics::default());
    let metered = MeteredStorage::new(&storage, &counters);
    let rows = collect_scan(
        metered
            .scan(ScanRequest::prefix("indices".into(), vec![]))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), keys.len());
    assert_eq!(
        *counters.borrow(),
        StorageReadMetrics {
            total: StorageReadBucket {
                reads: 7,
                ranges: 1
            },
            global_current_indexes: StorageReadBucket {
                reads: 2,
                ranges: 0
            },
            history_indexes: StorageReadBucket {
                reads: 1,
                ranges: 0
            },
            transactions_indexes: StorageReadBucket {
                reads: 1,
                ranges: 0
            },
            other: StorageReadBucket {
                reads: 3,
                ranges: 1
            },
            ..StorageReadMetrics::default()
        }
    );

    *counters.borrow_mut() = StorageReadMetrics::default();
    let rows = collect_scan(
        metered
            .scan(
                ScanRequest::prefix(
                    "indices".into(),
                    b"jazz_fixture_global_current\0by_app_owner\0".to_vec(),
                )
                .with_max_items(1),
            )
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        *counters.borrow(),
        StorageReadMetrics {
            total: StorageReadBucket {
                reads: 1,
                ranges: 1
            },
            global_current_indexes: StorageReadBucket {
                reads: 1,
                ranges: 1
            },
            ..StorageReadMetrics::default()
        }
    );
}
