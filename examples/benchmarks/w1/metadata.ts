import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

export const w1Benchmarks: BenchmarkMetadata[] = [];
const profileS = "3,000 tasks, 12,000 comments and 9,000 activity rows, plus users/projects.";
type Backend = "memory" | "rocksdb";
function add(
  name: string,
  backend: Backend,
  title: string,
  description: string,
  fixture: string,
  includes: string[],
  unit: string,
  explanation: string,
) {
  w1Benchmarks.push({
    name,
    title,
    description,
    fixture,
    includes,
    storage: backend === "memory" ? "In-memory Jazz database" : "RocksDB; WAL without fsync",
    excludes: [
      "Fixture seeding and database opening",
      "Real-network latency and JS/browser scheduling",
    ],
    work: { count: 1, unit, explanation },
    source: `examples/benchmarks/w1/benches/reads_${backend}_walltime.rs`,
  });
}
for (const backend of ["memory", "rocksdb"] as const) {
  add(
    `query_board_profile_s_${backend}`,
    backend,
    "W1 project board read",
    "Execute one prepared project-board read, filtering and ordering tasks for a project with a bounded result.",
    profileS,
    ["One one-shot read and result-row count; query preparation is outside timing"],
    "board reads/s",
    "One board-read operation per iteration.",
  );
  add(
    `query_task_detail_profile_s_${backend}`,
    backend,
    "W1 task detail reads",
    "Read a task's comments and activity using two prepared, ordered, bounded one-shot queries.",
    profileS,
    ["Both comments and activity reads and their result counts"],
    "task-detail operations/s",
    "One task-detail operation = two queries; this is not individual queries/s.",
  );
  add(
    `query_bounded_activity_page_profile_s_${backend}`,
    backend,
    "W1 bounded activity page",
    "Execute one activity-page read with two indexed equality predicates and LIMIT 50.",
    profileS,
    ["One prepared one-shot query and its result count"],
    "page reads/s",
    "One bounded-page query per iteration; table size is not the returned-row count.",
  );
  for (const activity of [9000, 30000])
    add(
      `query_bounded_activity_page_scaling_${backend}[${activity}]`,
      backend,
      "W1 bounded activity page · scaling",
      "Read a LIMIT 50 activity page using two equality predicates as the activity table grows.",
      `3,000 tasks, 12,000 comments and ${activity.toLocaleString("en-US")} activity rows.`,
      ["One prepared one-shot query and its result count"],
      "page reads/s",
      "One bounded-page query per iteration, regardless of the surrounding table size.",
    );
  for (const [tasks, comments, activity] of [
    [300, 1200, 900],
    [3000, 12000, 9000],
  ])
    add(
      `query_comments_scaling_${backend}[(${tasks}, ${comments}, ${activity})]`,
      backend,
      "W1 task comments · scaling",
      "Read ordered comments for one task, keeping per-task result cardinality fixed while scaling the surrounding tables.",
      `${tasks.toLocaleString("en-US")} tasks, ${comments.toLocaleString("en-US")} comments and ${activity.toLocaleString("en-US")} activity rows.`,
      ["One prepared comments query and result count"],
      "comment reads/s",
      "One comments-query operation per iteration; total fixture rows are not the numerator.",
    );
  add(
    `subscribe_activity_intersection_delta_${backend}`,
    backend,
    "W1 indexed subscription update",
    "Toggle one activity row's indexed kind and drain the resulting events from an already-hydrated maintained subscription.",
    profileS,
    ["One update, local settlement and maintained-event consumption"],
    "updates with delivery/s",
    "One update-and-delivery operation; not the number of result events.",
  );
  add(
    `update_activity_indexed_predicate_no_subscription_${backend}`,
    backend,
    "W1 indexed update without subscribers",
    "Toggle one activity row's indexed kind with no live subscription attached.",
    profileS,
    ["One update and local-durability settlement"],
    "updates/s",
    "One completed indexed-field update per iteration.",
  );
}
add(
  "update_activity_policy_no_subscription_memory",
  "memory",
  "W1 permissioned update without subscribers",
  "Update one activity row under a trusted-session, row-dependent SELECT/UPDATE policy, without a live subscription.",
  profileS,
  ["Policy evaluation, indexed-field update and local settlement"],
  "permissioned updates/s",
  "One policy-checked update per iteration.",
);
for (const activity of [900, 9000]) {
  const large = `3,000 tasks, 12,000 comments and ${activity.toLocaleString("en-US")} activity rows.`;
  const small = `300 tasks, 1,200 comments and ${activity.toLocaleString("en-US")} activity rows.`;
  add(
    `update_activity_policy_scaling_memory[${activity}]`,
    "memory",
    "W1 permissioned update · scaling",
    "Toggle an indexed field under a row-dependent SELECT/UPDATE policy as the surrounding activity table grows.",
    large,
    ["One policy-checked update and local settlement"],
    "permissioned updates/s",
    "One completed update per iteration.",
  );
  for (const policy of [false, true])
    add(
      `subscribe_activity_${policy ? "policy_" : ""}point_scaling_memory[${activity}]`,
      "memory",
      `W1 ${policy ? "permissioned " : ""}point subscription`,
      `Attach one point activity subscription and consume its initial result${policy ? " under the row-dependent activity policy" : ""}.`,
      small,
      ["Subscription attachment and initial-event consumption"],
      "subscriptions attached/s",
      "One initial point subscription per iteration; fixture row count is load context.",
    );
  add(
    `delete_task_point_scaling_memory[${activity}]`,
    "memory",
    "W1 point delete · scaling",
    "Delete one task and wait for local settlement in a freshly seeded fixture.",
    small,
    ["One task deletion and local settlement"],
    "deletes/s",
    "One settled delete per iteration.",
  );
  add(
    `restore_task_point_scaling_memory[${activity}]`,
    "memory",
    "W1 point restore · scaling",
    "Restore one previously deleted task and wait for local settlement. The deletion is fixture setup.",
    small,
    ["One task restoration and local settlement"],
    "restores/s",
    "One settled restore per iteration.",
  );
}
add(
  "resubscribe_activity_point_profile_s_memory",
  "memory",
  "W1 repeated point subscription",
  "Attach and consume a point subscription after an initial subscription has warmed the reusable fixture.",
  "300 tasks, 1,200 comments and 9,000 activity rows.",
  ["One repeated attachment and initial-result consumption"],
  "subscriptions attached/s",
  "One reattachment per iteration; this is a warm fixture, not process-cold load.",
);
for (const [tasks, comments, activity] of [
  [300, 1200, 900],
  [500, 2000, 1500],
])
  add(
    `resume_one_task_update_scaling_memory[(${tasks}, ${comments}, ${activity})]`,
    "memory",
    "W1 reconnect after one task update",
    "Reconnect the prepared byte-wire topology after one offline task update and consume the catch-up result. This is not a standalone update benchmark.",
    `${tasks} tasks, ${comments} comments and ${activity} activity rows; the prior subscription/topology is prepared outside timing.`,
    [
      "Connection/resume exchange, encoded delivery and receiver application",
      "Catch-up event consumption; validation boundaries depend on the measured harness revision",
    ],
    "reconnects/s",
    "One reconnect/catch-up per iteration. A single changed task does not imply a one-row wire response.",
  );

// Retired, unsuffixed memory names remain documented for historical receipts.
const legacyBenchmarks = w1Benchmarks
  .filter(
    (current) =>
      current.name === "query_board_profile_s_memory" ||
      current.name === "query_task_detail_profile_s_memory" ||
      current.name.startsWith("query_comments_scaling_memory["),
  )
  .map((current) => ({
    ...current,
    name: current.name.replace("_memory", ""),
    title: `${current.title} · legacy name`,
    description: `${current.description} Retired unsuffixed memory-series name; consult the measured commit for historical timing boundaries.`,
  }));
w1Benchmarks.push(...legacyBenchmarks);
