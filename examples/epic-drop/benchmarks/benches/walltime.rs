//! EpicDrop wall-clock receipts for what a user of the file browser notices:
//! how long an upload takes, how fast a folder lists, and how quickly a
//! download or a seek into a large file returns. Names are app-prefixed
//! because the examples page matches CodSpeed results by exact name.

use jazz_example_epic_drop_benchmark::{Fixture, UploadFixture};

const MIB: usize = 1024 * 1024;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

/// Stream one 4 MiB file from a bounded reader and wait for local durability.
#[divan::bench(sample_count = 20)]
fn epic_drop_upload_4mb(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(UploadFixture::new)
        .bench_local_values(|fixture| fixture.upload(0, 4 * MIB));
}

/// The same upload at 64 MiB: a long recording or a video.
#[divan::bench(sample_count = 5)]
fn epic_drop_upload_64mb(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(UploadFixture::new)
        .bench_local_values(|fixture| fixture.upload(0, 64 * MIB));
}

/// Open a folder of 100 files. Only metadata is read; file contents stay put.
#[divan::bench]
fn epic_drop_folder_listing_100_files(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::with_files(100, 256 * 1024);
    bencher.bench_local(|| divan::black_box(fixture.list_folder()));
}

/// Download a whole 4 MiB file.
#[divan::bench]
fn epic_drop_download_4mb(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new(4 * MIB);
    bencher.bench_local(|| divan::black_box(fixture.download_file()));
}

/// Seek into the middle of a 64 MiB file and read a 64 KiB window, as an
/// audio or video player does when the user scrubs.
#[divan::bench]
fn epic_drop_seek_64mb(bencher: divan::Bencher<'_, '_>) {
    let fixture = Fixture::new(64 * MIB);
    bencher.bench_local(|| divan::black_box(fixture.download_middle_range()));
}
