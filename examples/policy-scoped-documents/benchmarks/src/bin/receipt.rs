use jazz_example_policy_scoped_documents_benchmark::{
    Fixture, OWNERS, OWNERS_PER_ORG, Page, Policy, QUERY_OWNER, document_row, user,
};
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
    let requested_policy = std::env::args().nth(2);
    let composite_indexes = std::env::args().nth(3).as_deref() != Some("single");
    let deleted_rows: usize = std::env::args()
        .nth(4)
        .unwrap_or_else(|| "0".into())
        .parse()
        .expect("fourth argument: number of trailing document rows to delete");
    assert!(
        deleted_rows <= rows - rows / OWNERS * OWNERS_PER_ORG,
        "deletions must leave organization 0 intact"
    );
    let restore_deleted = std::env::args().nth(5).as_deref() == Some("restore");
    assert!(!restore_deleted || deleted_rows > 0, "nothing to restore");
    let scenario = std::env::args().nth(6).unwrap_or_else(|| "standard".into());
    assert!(
        matches!(scenario.as_str(), "standard" | "short" | "empty" | "sparse"),
        "sixth argument: standard, short, empty, or sparse"
    );
    let top_deleted_rows = if scenario == "sparse" { 20 } else { 0 };
    let org_rows = rows / OWNERS * OWNERS_PER_ORG;
    assert!(
        top_deleted_rows <= org_rows,
        "organization has fewer than 20 rows"
    );
    for policy in [Policy::Unrestricted, Policy::Owner, Policy::OwnerOrOrg]
        .into_iter()
        .filter(|policy| {
            requested_policy
                .as_deref()
                .is_none_or(|requested| format!("{policy:?}") == requested)
        })
    {
        let fixture = Fixture::with_composite_indexes(rows, policy, composite_indexes);
        let delete_start = Instant::now();
        if deleted_rows > 0 {
            let indices = (rows - deleted_rows..rows).collect::<Vec<_>>();
            fixture.delete_documents(&indices);
            if restore_deleted {
                fixture.restore_documents(&indices, |index| index as u64);
            }
        }
        if top_deleted_rows > 0 {
            fixture.delete_documents(&(org_rows - top_deleted_rows..org_rows).collect::<Vec<_>>());
        }
        let mutation_us = delete_start.elapsed().as_micros();
        let cases = match scenario.as_str() {
            "standard" => [Page::Owner(QUERY_OWNER), Page::Org(0)]
                .into_iter()
                .flat_map(|page| [1, 10, 50].map(|limit| (page, limit)))
                .collect::<Vec<_>>(),
            "short" => vec![(Page::Owner(QUERY_OWNER), rows / OWNERS + 50)],
            "empty" => vec![(Page::Org(OWNERS / OWNERS_PER_ORG), 10)],
            "sparse" => vec![(Page::Org(0), 10)],
            _ => unreachable!(),
        };
        for (page, limit) in cases {
            let mut session = fixture.session(page, limit, user(QUERY_OWNER));
            let start = Instant::now();
            let result = session.read();
            let query_us = start.elapsed().as_micros();
            let expected = match scenario.as_str() {
                "short" => Some(
                    (0..rows / OWNERS)
                        .rev()
                        .map(|index| document_row(QUERY_OWNER * rows / OWNERS + index))
                        .collect::<Vec<_>>(),
                ),
                "empty" => Some(Vec::new()),
                "sparse" => {
                    let last_visible = if policy == Policy::Owner {
                        (QUERY_OWNER + 1) * rows / OWNERS
                    } else {
                        org_rows - top_deleted_rows
                    };
                    Some(
                        (last_visible - 10..last_visible)
                            .rev()
                            .map(document_row)
                            .collect::<Vec<_>>(),
                    )
                }
                _ => None,
            };
            if let Some(expected) = expected {
                assert_eq!(
                    result.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
                    expected
                );
            }
            let metrics = session.take_metrics();
            let reopen_us = session.reopen_us;
            let storage_open_us = session.storage_open_us;
            let jazz_open_us = session.jazz_open_us;
            let open = session.open_receipt;
            let prepare_us = session.prepare_us;
            let start = Instant::now();
            drop(session);
            let close_us = start.elapsed().as_micros();
            println!(
                "{}",
                serde_json::json!({
                    "fixture_revision": 4, "table_rows": rows,
                    "scenario": scenario,
                    "deleted_rows": if restore_deleted { top_deleted_rows } else { deleted_rows + top_deleted_rows },
                    "unrelated_deleted_rows": if restore_deleted { 0 } else { deleted_rows },
                    "top_deleted_rows": top_deleted_rows,
                    "deletion_register_rows": deleted_rows + top_deleted_rows,
                    "restored": restore_deleted, "mutation_us": mutation_us,
                    "composite_indexes": composite_indexes,
                    "policy": format!("{policy:?}"), "page": format!("{page:?}"),
                    "limit": limit, "result_rows": result.len(),
                    "allocator": jazz_benchmark_guard::ALLOCATOR_NAME,
                    "seed_us": fixture.seed_us, "reopen_us": reopen_us,
                    "storage_open_us": storage_open_us, "jazz_open_us": jazz_open_us,
                    "open_phases_us": {
                        "catalogue": open.catalogue_open.as_micros(),
                        "database": open.database_open.as_micros(),
                        "state_init": open.state_init.as_micros(),
                        "recover_storage": open.recover_storage.as_micros(),
                        "recover_catalogue_state": open.recover_catalogue_state.as_micros(),
                        "recover_global_times": open.recover_global_times.as_micros(),
                        "recover_pending_and_rejected": open.recover_pending_and_rejected.as_micros(),
                        "recover_unclean_close": open.recover_unclean_close.as_micros(),
                        "recover_known_state": open.recover_known_state.as_micros(),
                        "rebuild_ahead_current": open.rebuild_ahead_current.as_micros(),
                        "finalize_catalogue": open.finalize_catalogue.as_micros(),
                        "global_time_records_scanned": open.global_time_records_scanned,
                        "ahead_current_entries": open.ahead_current_entries,
                    },
                    "prepare_us": prepare_us, "query_us": query_us, "close_us": close_us,
                    "index_reads": metrics.global_current_indexes.reads,
                    "current_row_reads": metrics.global_current_rows.reads,
                    "deletion_register_reads": metrics.register_global_current_rows.reads,
                    "deletion_register_ranges": metrics.register_global_current_rows.ranges,
                    "history_row_reads": metrics.history_rows.reads,
                    "total_logical_reads": metrics.total.reads,
                    "total_logical_ranges": metrics.total.ranges,
                    "other_reads": metrics.other.reads,
                    "other_ranges": metrics.other.ranges,
                })
            );
        }
    }
}
