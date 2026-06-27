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
#[path = "../benches/s6_text_traces.rs"]
mod s6_text_traces;
#[allow(dead_code)]
#[path = "../benches/s7_migrations.rs"]
mod s7_migrations;
#[allow(dead_code)]
#[path = "../benches/s9_durable_execution.rs"]
mod s9_durable_execution;

#[test]
fn s1_saas_smoke() {
    s1_saas::smoke();
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
fn s6_text_traces_smoke() {
    s6_text_traces::smoke();
}

#[test]
fn s7_migrations_smoke() {
    s7_migrations::smoke();
}

#[test]
fn s9_durable_execution_smoke() {
    s9_durable_execution::smoke();
}
