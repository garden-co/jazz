//! Smoke test for BfTree on mobile targets (iOS / Android).
//!
//! Exercises the core operations — open, insert, read, delete, flush — using
//! StdVfs (the filesystem backend available on all non-WASM Unix targets).
//!
//! Run on host (proxy for mobile code path):
//!     cargo run -p bf-tree --example mobile_smoke_test
//!
//! Run on iOS simulator (requires Xcode + a booted simulator):
//!     cargo build -p bf-tree --example mobile_smoke_test --target aarch64-apple-ios-sim
//!     xcrun simctl spawn booted target/aarch64-apple-ios-sim/debug/examples/mobile_smoke_test
//!
//! Run on Android emulator (requires NDK + cargo-ndk + a booted emulator):
//!     cargo ndk -t arm64-v8a build -p bf-tree --example mobile_smoke_test
//!     adb push target/aarch64-linux-android/debug/examples/mobile_smoke_test /data/local/tmp/
//!     adb shell /data/local/tmp/mobile_smoke_test
//!
//! Options:
//!     --memory       Use in-memory backend (no filesystem)
//!     --bulk N       Number of bulk insert entries (default: 10000)
//!
//! Known issue: on Android debug builds, bulk inserts that trigger leaf node
//! splits (~43+ entries) crash due to a pre-existing UB in mapping_table.rs
//! (get_unchecked_mut out of bounds). The Android debug sysroot includes
//! runtime UB precondition checks that catch this; macOS does not.
//! See mapping_table.rs:116. This needs fixing before production use.

use bf_tree::{BfTree, LeafInsertResult, LeafReadResult};

fn main() {
    let use_memory = std::env::args().any(|a| a == "--memory");

    let dir = std::env::temp_dir().join("bf_tree_mobile_smoke_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");

    let db_path = dir.join("test.db");
    println!("=== BfTree mobile smoke test ===");

    // --- Open ---
    let cache_size = 4 * 1024 * 1024; // 4 MiB
    let tree = if use_memory {
        println!("mode: in-memory (no WAL, no filesystem)");
        BfTree::new(":memory:", cache_size).expect("open tree")
    } else {
        println!("mode: file-backed (StdVfs)");
        println!("db path: {}", db_path.display());
        BfTree::new(&db_path, cache_size).expect("open tree")
    };
    println!("[ok] open");

    // --- Insert ---
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"alice", b"alice@jazz.tools"),
        (b"bob", b"bob@jazz.tools"),
        (b"carol", b"carol@jazz.tools"),
    ];

    for (key, value) in &entries {
        match tree.insert(key, value) {
            LeafInsertResult::Success => {}
            other => panic!("insert {:?} failed: {:?}", std::str::from_utf8(key), other),
        }
    }
    println!("[ok] insert {} entries", entries.len());

    // --- Read back ---
    let mut buf = vec![0u8; 4096];
    for (key, expected) in &entries {
        match tree.read(key, &mut buf) {
            LeafReadResult::Found(len) => {
                let actual = &buf[..len as usize];
                assert_eq!(actual, *expected, "value mismatch for key {:?}", key);
            }
            other => panic!("read {:?} => {:?}, expected Found", key, other),
        }
    }
    println!("[ok] read verified");

    // --- Delete ---
    tree.delete(b"bob");
    match tree.read(b"bob", &mut buf) {
        LeafReadResult::NotFound | LeafReadResult::Deleted => {}
        other => panic!("expected bob deleted, got {:?}", other),
    }
    assert!(matches!(tree.read(b"alice", &mut buf), LeafReadResult::Found(_)));
    assert!(matches!(tree.read(b"carol", &mut buf), LeafReadResult::Found(_)));
    println!("[ok] delete + verify survivors");

    // --- Overwrite ---
    match tree.insert(b"alice", b"alice-v2@jazz.tools") {
        LeafInsertResult::Success => {}
        other => panic!("overwrite failed: {:?}", other),
    }
    match tree.read(b"alice", &mut buf) {
        LeafReadResult::Found(len) => {
            assert_eq!(&buf[..len as usize], b"alice-v2@jazz.tools");
        }
        other => panic!("read after overwrite: {:?}", other),
    }
    println!("[ok] overwrite");

    // --- Bulk insert ---
    let bulk_count: usize = std::env::args()
        .position(|a| a == "--bulk")
        .and_then(|pos| std::env::args().nth(pos + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    println!("bulk inserting {} entries...", bulk_count);
    for i in 0..bulk_count {
        let key = format!("key-{:06}", i);
        let val = format!("val-{:06}-{}", i, "x".repeat(64));
        tree.insert(key.as_bytes(), val.as_bytes());
    }
    // Spot-check (only indices that exist)
    for i in [0, 42, 999, 5000, 9999].iter().copied().filter(|&i| i < bulk_count) {
        let key = format!("key-{:06}", i);
        let expected_prefix = format!("val-{:06}-", i);
        match tree.read(key.as_bytes(), &mut buf) {
            LeafReadResult::Found(len) => {
                let actual = std::str::from_utf8(&buf[..len as usize]).unwrap();
                assert!(
                    actual.starts_with(&expected_prefix),
                    "bulk key {} mismatch: {}",
                    key,
                    actual
                );
            }
            other => panic!("bulk read {} => {:?}", key, other),
        }
    }
    println!("[ok] bulk insert + spot-check ({} entries)", bulk_count);

    // --- Cleanup ---
    drop(tree);
    let _ = std::fs::remove_dir_all(&dir);

    println!("=== all passed ===");
}
