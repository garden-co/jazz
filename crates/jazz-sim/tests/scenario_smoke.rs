//! Each scenario's deterministic correctness run, as an ordinary test.
//! Timing is the benchmark binaries' job; these assert the oracles.

#[allow(dead_code)]
#[path = "../benches/micro.rs"]
mod micro;

#[test]
fn micro_correctness_smoke() {
    micro::correctness_smoke();
}

#[allow(dead_code)]
#[path = "../benches/s1_saas.rs"]
mod s1_saas;

#[test]
fn s1_saas_smoke() {
    s1_saas::smoke();
}

#[test]
fn s1_saas_db_surface_smoke() {
    s1_saas::db_surface_smoke();
}

#[allow(dead_code)]
#[path = "../benches/s2_canvas.rs"]
mod s2_canvas;

#[test]
fn s2_canvas_smoke() {
    s2_canvas::smoke();
}

#[allow(dead_code)]
#[path = "../benches/s3_permissions.rs"]
mod s3_permissions;

#[test]
fn s3_permissions_smoke() {
    s3_permissions::smoke();
}

#[allow(dead_code)]
#[path = "../benches/s4_order_processing.rs"]
mod s4_order_processing;

#[test]
fn s4_order_processing_smoke_debug_profile() {
    s4_order_processing::smoke();
}

#[allow(dead_code)]
#[path = "../benches/s5_durable_stream.rs"]
mod s5_durable_stream;

#[test]
fn s5_durable_stream_smoke() {
    s5_durable_stream::smoke();
}

#[allow(dead_code)]
#[path = "../benches/s7_migrations.rs"]
mod s7_migrations;

#[test]
fn s7_migrations_smoke() {
    s7_migrations::smoke();
}

#[allow(dead_code)]
#[path = "../benches/s8_branch_views.rs"]
mod s8_branch_views;

#[test]
fn s8_branch_views_smoke() {
    s8_branch_views::smoke();
}

#[allow(dead_code)]
#[path = "../benches/s9_durable_execution.rs"]
mod s9_durable_execution;

#[test]
fn s9_durable_execution_smoke() {
    s9_durable_execution::smoke();
}
