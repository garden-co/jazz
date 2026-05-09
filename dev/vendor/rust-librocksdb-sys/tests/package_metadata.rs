#[test]
fn package_metadata_matches_zaidoon_rust_rocksdb_v0_48() {
    assert_eq!(env!("CARGO_PKG_NAME"), "rust-librocksdb-sys");
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.44.0+11.1.1");
}
