//! Counters for the disabled `cold-settle-attribution` benchmark feature.
//!
//! These counters classify sender preflight serialization and oversized view
//! update splitting. They are process-global because the benchmark creates
//! several databases in one process.

#![allow(missing_docs)]

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub preflight_payload_encodes: u64,
    pub preflight_payload_encode_ns: u64,
    pub preflight_payload_bytes: u64,
    pub preflight_frame_encodes: u64,
    pub preflight_frame_encode_ns: u64,
    pub preflight_frame_bytes: u64,
    pub view_updates_fit: u64,
    pub view_updates_split: u64,
    pub chunks_emitted: u64,
    pub candidate_builds: u64,
    pub candidate_build_ns: u64,
    pub candidate_encoded_bytes: u64,
    pub selected_payloads: u64,
    pub selected_payload_bytes: u64,
}

macro_rules! counter_fields {
    ($($name:ident),+ $(,)?) => {
        $(static $name: AtomicU64 = AtomicU64::new(0);)+

        pub fn reset() {
            $($name.store(0, Ordering::Relaxed);)+
        }
    };
}

counter_fields!(
    PREFLIGHT_PAYLOAD_ENCODES,
    PREFLIGHT_PAYLOAD_ENCODE_NS,
    PREFLIGHT_PAYLOAD_BYTES,
    PREFLIGHT_FRAME_ENCODES,
    PREFLIGHT_FRAME_ENCODE_NS,
    PREFLIGHT_FRAME_BYTES,
    VIEW_UPDATES_FIT,
    VIEW_UPDATES_SPLIT,
    CHUNKS_EMITTED,
    CANDIDATE_BUILDS,
    CANDIDATE_BUILD_NS,
    CANDIDATE_ENCODED_BYTES,
    SELECTED_PAYLOADS,
    SELECTED_PAYLOAD_BYTES,
);

pub fn snapshot() -> Snapshot {
    Snapshot {
        preflight_payload_encodes: PREFLIGHT_PAYLOAD_ENCODES.load(Ordering::Relaxed),
        preflight_payload_encode_ns: PREFLIGHT_PAYLOAD_ENCODE_NS.load(Ordering::Relaxed),
        preflight_payload_bytes: PREFLIGHT_PAYLOAD_BYTES.load(Ordering::Relaxed),
        preflight_frame_encodes: PREFLIGHT_FRAME_ENCODES.load(Ordering::Relaxed),
        preflight_frame_encode_ns: PREFLIGHT_FRAME_ENCODE_NS.load(Ordering::Relaxed),
        preflight_frame_bytes: PREFLIGHT_FRAME_BYTES.load(Ordering::Relaxed),
        view_updates_fit: VIEW_UPDATES_FIT.load(Ordering::Relaxed),
        view_updates_split: VIEW_UPDATES_SPLIT.load(Ordering::Relaxed),
        chunks_emitted: CHUNKS_EMITTED.load(Ordering::Relaxed),
        candidate_builds: CANDIDATE_BUILDS.load(Ordering::Relaxed),
        candidate_build_ns: CANDIDATE_BUILD_NS.load(Ordering::Relaxed),
        candidate_encoded_bytes: CANDIDATE_ENCODED_BYTES.load(Ordering::Relaxed),
        selected_payloads: SELECTED_PAYLOADS.load(Ordering::Relaxed),
        selected_payload_bytes: SELECTED_PAYLOAD_BYTES.load(Ordering::Relaxed),
    }
}

pub fn record_preflight_payload(elapsed_ns: u64, bytes: usize) {
    PREFLIGHT_PAYLOAD_ENCODES.fetch_add(1, Ordering::Relaxed);
    PREFLIGHT_PAYLOAD_ENCODE_NS.fetch_add(elapsed_ns, Ordering::Relaxed);
    PREFLIGHT_PAYLOAD_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn record_preflight_frame(elapsed_ns: u64, bytes: usize) {
    PREFLIGHT_FRAME_ENCODES.fetch_add(1, Ordering::Relaxed);
    PREFLIGHT_FRAME_ENCODE_NS.fetch_add(elapsed_ns, Ordering::Relaxed);
    PREFLIGHT_FRAME_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn record_view_update_fit(encoded_bytes: usize) {
    VIEW_UPDATES_FIT.fetch_add(1, Ordering::Relaxed);
    CHUNKS_EMITTED.fetch_add(1, Ordering::Relaxed);
    SELECTED_PAYLOADS.fetch_add(1, Ordering::Relaxed);
    SELECTED_PAYLOAD_BYTES.fetch_add(encoded_bytes as u64, Ordering::Relaxed);
}

pub fn record_view_update_split() {
    VIEW_UPDATES_SPLIT.fetch_add(1, Ordering::Relaxed);
}

pub fn record_candidate(encoded_bytes: usize, build_ns: u64) {
    CANDIDATE_BUILDS.fetch_add(1, Ordering::Relaxed);
    CANDIDATE_ENCODED_BYTES.fetch_add(encoded_bytes as u64, Ordering::Relaxed);
    CANDIDATE_BUILD_NS.fetch_add(build_ns, Ordering::Relaxed);
}

pub fn record_selected_payload(encoded_bytes: usize) {
    CHUNKS_EMITTED.fetch_add(1, Ordering::Relaxed);
    SELECTED_PAYLOADS.fetch_add(1, Ordering::Relaxed);
    SELECTED_PAYLOAD_BYTES.fetch_add(encoded_bytes as u64, Ordering::Relaxed);
}
