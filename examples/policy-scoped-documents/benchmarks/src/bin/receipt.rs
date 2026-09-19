use jazz_example_policy_scoped_documents_benchmark::{Fixture, Page, Policy, QUERY_OWNER, user};
use std::time::Instant;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let rows = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "10000".into())
        .parse()
        .expect("first argument: row count, positive multiple of 100");
    for policy in [Policy::Unrestricted, Policy::Owner, Policy::OwnerOrOrg] {
        let fixture = Fixture::new(rows, policy);
        for page in [Page::Owner(QUERY_OWNER), Page::Org(0)] {
            for limit in [1, 10, 50] {
                let mut session = fixture.session(page, limit, user(QUERY_OWNER));
                let start = Instant::now();
                let result = session.read();
                let query_us = start.elapsed().as_micros();
                let metrics = session.take_metrics();
                let reopen_us = session.reopen_us;
                let prepare_us = session.prepare_us;
                let start = Instant::now();
                drop(session);
                let close_us = start.elapsed().as_micros();
                println!(
                    "{}",
                    serde_json::json!({
                        "fixture_revision": 2, "table_rows": rows,
                        "policy": format!("{policy:?}"), "page": format!("{page:?}"),
                        "limit": limit, "result_rows": result.len(),
                        "allocator": jazz_benchmark_guard::ALLOCATOR_NAME,
                        "seed_us": fixture.seed_us, "reopen_us": reopen_us,
                        "prepare_us": prepare_us, "query_us": query_us, "close_us": close_us,
                        "index_reads": metrics.global_current_indexes.reads,
                        "current_row_reads": metrics.global_current_rows.reads,
                        "history_row_reads": metrics.history_rows.reads,
                        "total_logical_reads": metrics.total.reads,
                    })
                );
            }
        }
    }
}
