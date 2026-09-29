/**
 * CodSpeed names retired when the benchmarks were consolidated into the hero
 * examples, mapped to the case that now measures the same workload (null when
 * the case moved to a `nightly` bench target or was dropped). CodSpeed history
 * restarts under the new names; results recorded under a former name, such as
 * reviewed release backfills, remain valid history for that former name.
 */
export const formerBenchmarkNames: ReadonlyMap<string, string | null> = new Map([
  ["sequential_insert_1350_rocksdb", "stage_plan_add_task_1350"],
  ["sequential_update_1350_rocksdb", "stage_plan_check_off_task_1350"],
  ["batch_update_1350_rocksdb", "stage_plan_bulk_complete_1350"],
  ["reopen_1500_rocksdb", "stage_plan_reopen_1500"],
  ["subscription_fanout_memory[(600, 0)]", null],
  ["subscription_fanout_memory[(600, 10)]", null],
  ["subscription_fanout_memory[(600, 60)]", "stage_plan_crew_dashboard[(600, 60)]"],
  ["subscription_fanout_memory[(6000, 60)]", "stage_plan_crew_dashboard[(6000, 60)]"],
  ["policy_free_org_page50[100000]", "band_book_workspace_pages_unrestricted[100000]"],
  ["owner_or_org_policy_org_page50[100000]", "band_book_workspace_pages[100000]"],
  ["owner_policy_page50[100000]", "band_book_my_pages[100000]"],
  ["subscribe_owner_or_org_policy_org_page50[100000]", "band_book_workspace_pages_live[100000]"],
  ...[
    "policy_free_owner_page50",
    "owner_policy_page50",
    "owner_or_org_policy_owner_page50",
    "policy_free_org_page50",
    "owner_or_org_policy_org_page50",
    "subscribe_policy_free_owner_page50",
    "subscribe_owner_policy_page50",
    "subscribe_owner_or_org_policy_owner_page50",
    "subscribe_policy_free_org_page50",
    "subscribe_owner_or_org_policy_org_page50",
  ].map((name) => [`${name}[10000]`, null] as const),
  ...[
    "policy_free_owner_page50",
    "owner_or_org_policy_owner_page50",
    "subscribe_policy_free_owner_page50",
    "subscribe_owner_policy_page50",
    "subscribe_owner_or_org_policy_owner_page50",
    "subscribe_policy_free_org_page50",
  ].map((name) => [`${name}[100000]`, null] as const),
]);
