//! Native RocksDB receipt for the ordinary-row file layout.
//!
//! `JAZZ_FILE_DISK_BENCH=1 cargo bench -p groove --bench file_layout_storage`
//! reports logical bytes, append latency, and both apparent (`metadata.len`)
//! and allocated (`st_blocks * 512`, Unix) directory bytes. RocksDB's WAL and
//! memtables make pre-flush numbers deliberately non-final; use the reported
//! post-flush and post-compaction receipts for comparisons.
use rocksdb::{ColumnFamilyDescriptor, DB, DBCompressionType, Options, WriteBatch};
use serde::Serialize;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{fs, path::Path, time::Instant};
use tempfile::TempDir;

const PART: usize = 256 * 1024;
#[derive(Serialize)]
struct Bytes {
    apparent: u64,
    allocated: Option<u64>,
}
#[derive(Serialize)]
struct Receipt {
    appends: usize,
    append_bytes: usize,
    logical_bytes: usize,
    p50_us: u128,
    p95_us: u128,
    root_rows: usize,
    part_rows: usize,
    compression: &'static str,
    before_flush: Bytes,
    after_flush: Bytes,
    after_compaction: Bytes,
    caveat: &'static str,
}
fn bytes(path: &Path) -> Bytes {
    fn walk(p: &Path, a: &mut u64, #[cfg(unix)] b: &mut u64) {
        let Ok(rd) = fs::read_dir(p) else { return };
        for e in rd.flatten() {
            let Ok(m) = e.metadata() else { continue };
            if m.is_dir() {
                walk(
                    &e.path(),
                    a,
                    #[cfg(unix)]
                    b,
                )
            } else {
                *a += m.len();
                #[cfg(unix)]
                {
                    *b += m.blocks() * 512;
                }
            }
        }
    }
    let mut a = 0;
    #[cfg(unix)]
    let mut b = 0;
    walk(
        path,
        &mut a,
        #[cfg(unix)]
        &mut b,
    );
    Bytes {
        apparent: a,
        allocated: {
            #[cfg(unix)]
            {
                Some(b)
            }
            #[cfg(not(unix))]
            {
                None
            }
        },
    }
}
fn main() {
    if std::env::var("JAZZ_FILE_DISK_BENCH").as_deref() != Ok("1") {
        eprintln!("skipped; set JAZZ_FILE_DISK_BENCH=1");
        return;
    }
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let appends = std::env::var("JAZZ_FILE_DISK_APPENDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    let append_bytes = std::env::var("JAZZ_FILE_DISK_APPEND_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(PART);
    let dir = TempDir::new().expect("tempdir");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    let mut history = Options::default();
    history.set_compression_type(DBCompressionType::Zstd);
    history.set_bottommost_compression_type(DBCompressionType::Zstd);
    let mut current_meta = Options::default();
    current_meta.set_compression_type(DBCompressionType::Lz4);
    current_meta.set_bottommost_compression_type(DBCompressionType::Zstd);
    let cfs = [
        ColumnFamilyDescriptor::new("roots", current_meta),
        ColumnFamilyDescriptor::new("parts", history),
    ];
    let db = DB::open_cf_descriptors(&opts, dir.path(), cfs).expect("rocksdb");
    let payload = vec![b'x'; append_bytes];
    let mut times = Vec::with_capacity(appends);
    for i in 0..appends {
        let now = Instant::now();
        let mut b = WriteBatch::default();
        for (n, chunk) in payload.chunks(PART).enumerate() {
            b.put_cf(
                db.cf_handle("parts").unwrap(),
                [(i as u64).to_be_bytes(), (n as u64).to_be_bytes()].concat(),
                chunk,
            );
        }
        b.put_cf(
            db.cf_handle("roots").unwrap(),
            (i as u64).to_be_bytes(),
            [
                (i as u64).to_be_bytes(),
                (payload.len() as u64).to_be_bytes(),
            ]
            .concat(),
        );
        db.write(&b).expect("ordinary-row file transaction");
        times.push(now.elapsed().as_micros());
    }
    times.sort_unstable();
    let before = bytes(dir.path());
    db.flush().expect("flush");
    let flushed = bytes(dir.path());
    db.compact_range::<&[u8], &[u8]>(None, None);
    let compacted = bytes(dir.path());
    let pct = |n: usize| times[(times.len() - 1) * n / 100];
    println!("{}",serde_json::to_string(&Receipt{appends,append_bytes,logical_bytes:appends*append_bytes,p50_us:pct(50),p95_us:pct(95),root_rows:appends,part_rows:appends*payload.len().div_ceil(PART),compression:"roots_lz4_parts_zstd_bottommost_zstd",before_flush:before,after_flush:flushed,after_compaction:compacted,caveat:"apparent/allocated bytes include RocksDB metadata and WAL; compaction timing is backend-dependent"}).unwrap());
}
