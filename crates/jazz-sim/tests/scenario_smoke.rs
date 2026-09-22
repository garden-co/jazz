#[allow(dead_code)]
#[path = "../benches/micro.rs"]
mod micro;
#[allow(dead_code)]
#[path = "../benches/s1_saas.rs"]
mod s1_saas;
#[allow(dead_code)]
#[path = "../benches/s2_canvas.rs"]
mod s2_canvas;
#[allow(dead_code)]
#[path = "../benches/s3_permissions.rs"]
mod s3_permissions;
#[allow(dead_code)]
#[path = "../benches/s4_order_processing.rs"]
mod s4_order_processing;
#[allow(dead_code)]
#[path = "../benches/s5_durable_stream.rs"]
mod s5_durable_stream;
#[allow(dead_code)]
#[path = "../benches/s7_migrations.rs"]
mod s7_migrations;
#[allow(dead_code)]
#[path = "../benches/s8_branch_views.rs"]
mod s8_branch_views;
#[allow(dead_code)]
#[path = "../benches/s9_durable_execution.rs"]
mod s9_durable_execution;

#[test]
fn s1_saas_smoke() {
    s1_saas::smoke();
}

#[test]
fn micro_correctness_smoke() {
    micro::correctness_smoke();
}

#[test]
fn s1_saas_db_surface_smoke() {
    s1_saas::db_surface_smoke();
}

#[test]
fn s2_canvas_smoke() {
    s2_canvas::smoke();
}

#[test]
fn s3_permissions_smoke() {
    s3_permissions::smoke();
}

#[test]
fn s4_order_processing_smoke_debug_profile() {
    s4_order_processing::smoke();
}

#[test]
fn s5_durable_stream_smoke() {
    s5_durable_stream::smoke();
}

#[test]
fn s7_migrations_smoke() {
    s7_migrations::smoke();
}

#[test]
fn s8_branch_views_smoke() {
    // This is the legacy smoke size, but runs as an ordinary deterministic
    // test rather than through a timed benchmark binary.
    s8_branch_views::run(64);
}

#[test]
fn s9_durable_execution_smoke() {
    s9_durable_execution::smoke();
}
