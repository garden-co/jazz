use std::hint::black_box;
use std::time::{Duration, Instant};

use jazz::large_values::{
    BytePatch, ContentTree, LargeValue, LargeValueOwnerDomain, MemoryLargeValueNodeRows,
    TailBounds, ValueEdit, ValueKind, ValueSelection,
};

fn elapsed_us(start: Instant) -> u128 {
    start.elapsed().as_micros()
}

fn mib_per_second(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

fn deterministic_bytes(size: usize) -> Vec<u8> {
    let mut state = 0x4c61_7267_6556_616c_u64;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn deterministic_text(size: usize) -> String {
    deterministic_bytes(size)
        .into_iter()
        .map(|byte| char::from(b'a' + byte % 26))
        .collect()
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();

    let size = std::env::var("JAZZ_LARGE_VALUE_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1024 * 1024);
    let bytes = deterministic_bytes(size);
    let domain =
        LargeValueOwnerDomain::new("benchmark_values", uuid::Uuid::from_bytes([9; 16]), "value")
            .unwrap();
    let tree = ContentTree::new(Default::default()).unwrap();
    let mut store = MemoryLargeValueNodeRows::default();

    let start = Instant::now();
    let mut value = LargeValue::create(
        ValueKind::Bytes,
        &domain,
        bytes.clone(),
        8 * 1024,
        tree,
        &mut store,
    )
    .unwrap();
    let create_elapsed = start.elapsed();
    let nodes_after_create = store.len();

    let start = Instant::now();
    for edit in 0..32_u64 {
        let operation = ValueEdit::Bytes(BytePatch::insert((size as u64 / 2) + edit, [b'x']));
        let patch = value
            .lower_edit(ValueKind::Bytes, operation, &domain, tree, &store)
            .unwrap();
        value = value
            .apply_edit(
                ValueKind::Bytes,
                &domain,
                patch,
                8 * 1024,
                TailBounds::default(),
                tree,
                &mut store,
            )
            .unwrap();
    }
    let edit_us = elapsed_us(start);

    let start = Instant::now();
    let range = value
        .select(
            ValueKind::Bytes,
            &ValueSelection::ByteRange {
                offset: size as u64 / 2,
                len: 4096,
            },
            &domain,
            tree,
            &store,
        )
        .unwrap();
    black_box(range);
    let range_us = elapsed_us(start);

    let start = Instant::now();
    let materialized = value
        .materialize(ValueKind::Bytes, &domain, tree, &store)
        .unwrap();
    black_box(&materialized);
    let materialize_elapsed = start.elapsed();

    let nodes_before_consolidate = store.len();
    let start = Instant::now();
    value = value
        .apply_edit(
            ValueKind::Bytes,
            &domain,
            BytePatch::insert(size as u64 / 3, b"consolidate"),
            8 * 1024,
            TailBounds {
                max_entries: 32,
                max_encoded_bytes: 16 * 1024,
            },
            tree,
            &mut store,
        )
        .unwrap();
    black_box(&value);
    let consolidate_us = elapsed_us(start);

    println!(
        "{}",
        serde_json::json!({
            "scenario": "large_value_pipeline",
            "bytes": size,
            "create_us": create_elapsed.as_micros(),
            "create_mib_per_s": mib_per_second(size, create_elapsed),
            "nodes_after_create": nodes_after_create,
            "edit_count": 32,
            "edit_tail_total_us": edit_us,
            "edit_tail_us_per_edit": edit_us as f64 / 32.0,
            "range_bytes": 4096,
            "range_us": range_us,
            "materialize_us": materialize_elapsed.as_micros(),
            "materialize_mib_per_s": mib_per_second(materialized.len(), materialize_elapsed),
            "consolidate_us": consolidate_us,
            "new_nodes_on_consolidate": store.len() - nodes_before_consolidate,
        })
    );

    let json_source = serde_json::to_vec(&serde_json::json!({
        "meta": { "version": 1 },
        "payload": deterministic_text(size),
    }))
    .unwrap();
    let mut json_store = MemoryLargeValueNodeRows::default();
    let start = Instant::now();
    let json_value = LargeValue::create(
        ValueKind::Json,
        &domain,
        json_source.clone(),
        8 * 1024,
        tree,
        &mut json_store,
    )
    .unwrap();
    let json_create_elapsed = start.elapsed();

    let start = Instant::now();
    let selected = json_value
        .select(
            ValueKind::Json,
            &ValueSelection::JsonPointer("/meta/version".to_owned()),
            &domain,
            tree,
            &json_store,
        )
        .unwrap();
    black_box(selected);
    let json_pointer_us = elapsed_us(start);

    let replacement = serde_json::json!({
        "meta": { "version": 2 },
        "payload": deterministic_text(size),
    });
    let start = Instant::now();
    let patch = json_value
        .lower_edit(
            ValueKind::Json,
            ValueEdit::Json(replacement),
            &domain,
            tree,
            &json_store,
        )
        .unwrap();
    let json_diff_us = elapsed_us(start);
    let inserted_patch_bytes = patch.insert.len();

    println!(
        "{}",
        serde_json::json!({
            "scenario": "large_json_pipeline",
            "bytes": json_source.len(),
            "create_us": json_create_elapsed.as_micros(),
            "create_mib_per_s": mib_per_second(json_source.len(), json_create_elapsed),
            "nodes_after_create": json_store.len(),
            "json_pointer_us": json_pointer_us,
            "json_replace_diff_us": json_diff_us,
            "json_replace_insert_bytes": inserted_patch_bytes,
        })
    );
}
