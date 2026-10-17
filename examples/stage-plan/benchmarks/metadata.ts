import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/stage-plan/benchmarks/benches/walltime.rs";

// --- One show's task list: RocksDB worker + in-memory foreground (former todo suite) ---
const taskList = {
  storage: "RocksDB worker + in-memory foreground; WAL without fsync",
  source,
  excludes: [
    "Fixture seeding",
    "Exact task-ID correctness verification",
    "Browser/JS scheduling, authentication bootstrap and real-network latency",
  ],
};

// --- The show board (former W1 workload) ---
const profileS =
  "3,000 tasks across 30 shows, 12,000 discussion comments and 9,000 activity rows, plus 10 crew members.";
const board = (
  name: string,
  backend: "memory" | "rocksdb",
  title: string,
  description: string,
  fixture: string,
  includes: string[],
  unit: string,
  explanation: string,
): BenchmarkMetadata => ({
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
  source,
});

export const stagePlanBenchmarks: BenchmarkMetadata[] = [
  {
    ...taskList,
    name: "stage_plan_add_task_1350",
    title: "StagePlan · add tasks one at a time",
    description:
      "Add 1,350 stage-prep tasks in separate transactions, completing foreground-to-worker-to-foreground delivery before adding the next.",
    fixture: "Start with 150 subscribed tasks; grow to 1,500. Deterministic caller-supplied IDs.",
    includes: [
      "Row authoring and persistence",
      "Upload encoding, worker ingest/publication and foreground application",
      "One final read of all 1,500 tasks",
    ],
    work: {
      count: 1350,
      unit: "inserts/s",
      explanation:
        "1,350 completed insert transactions per timed iteration; end-to-end workload rate, not a raw storage primitive.",
    },
  },
  {
    ...taskList,
    name: "stage_plan_check_off_task_1350",
    title: "StagePlan · check off tasks one at a time",
    description:
      "Mark 1,350 tasks done in separate transactions, with the worker roundtrip and foreground delivery after each.",
    fixture: "1,500 tasks remain present throughout; 1,350 are updated.",
    includes: [
      "Foreground authoring and persistence",
      "Worker roundtrip with encoding/decoding and subscription delivery",
      "One final read of all tasks",
    ],
    work: {
      count: 1350,
      unit: "updates/s",
      explanation: "1,350 completed update transactions per timed iteration.",
    },
  },
  {
    ...taskList,
    name: "stage_plan_bulk_complete_1350",
    title: "StagePlan · complete 1,350 tasks at once",
    description:
      "Mark 1,350 tasks done in one transaction, including the worker roundtrip and foreground delivery.",
    fixture: "1,500 subscribed tasks; one batch changes 90% of them.",
    includes: [
      "Batch authoring, upload encoding and worker ingest",
      "Publication and delivery back to the foreground",
      "One final read of all tasks",
    ],
    work: {
      count: 1350,
      unit: "rows updated/s",
      explanation:
        "1,350 changed rows per iteration, NOT 1,350 transactions. The batch is one transaction.",
    },
  },
  {
    ...taskList,
    name: "stage_plan_reopen_1500",
    title: "StagePlan · reopen with 1,500 tasks",
    description:
      "Reopen a seeded RocksDB worker, publish its tasks into an empty memory foreground, and read the complete result.",
    fixture: "1,500 persisted tasks; the foreground starts empty.",
    includes: ["Worker opening", "Publication, foreground ingest and final task query"],
    work: {
      count: 1500,
      unit: "visible rows/s",
      explanation:
        "1,500 rows made visible across one reopen-and-read operation, including fixed opening cost.",
    },
  },
  board(
    "stage_plan_open_board",
    "rocksdb",
    "StagePlan · open a show's board",
    "Execute one prepared board read, filtering and ordering one show's tasks with a bounded result.",
    profileS,
    ["One one-shot read and result-row count; query preparation is outside timing"],
    "board reads/s",
    "One board-read operation per iteration.",
  ),
  board(
    "stage_plan_open_task_detail",
    "rocksdb",
    "StagePlan · open a task",
    "Read a task's discussion and activity using two prepared, ordered, bounded one-shot queries.",
    profileS,
    ["Both discussion and activity reads and their result counts"],
    "task-detail operations/s",
    "One task-detail operation = two queries; this is not individual queries/s.",
  ),
  board(
    "stage_plan_activity_page[30000]",
    "rocksdb",
    "StagePlan · activity page from a large log",
    "Read a 50-row activity page using two indexed equality predicates from a 30,000-row activity log. Page cost should not depend on the log's size (#2026).",
    "3,000 tasks, 12,000 comments and 30,000 activity rows.",
    ["One prepared one-shot query and its result count"],
    "page reads/s",
    "One bounded-page query per iteration, regardless of the surrounding table size.",
  ),
  board(
    "stage_plan_move_card_to_done",
    "rocksdb",
    "StagePlan · move a card to Done",
    "Flip one row's indexed status so it enters or leaves an already-hydrated live filtered view, and drain the resulting events.",
    profileS,
    ["One update, local settlement and maintained-event consumption"],
    "moves with delivery/s",
    "One update-and-delivery operation; not the number of result events.",
  ),
  ...[
    [600, 60],
    [6000, 60],
  ].map(
    ([rows, lists]): BenchmarkMetadata => ({
      name: `stage_plan_crew_dashboard[(${rows}, ${lists})]`,
      title: `StagePlan · crew dashboard with ${lists} live lists`,
      description:
        "Open a crew member's dashboard: one overview plus 60 department lists, each an independent subscription through Core, a scope-isolated relay and a foreground; wait for every list to settle.",
      fixture: `Two crews of ${rows.toLocaleString("en-US")} tasks each; 60 departments per crew. Inherited crew-member permission excludes the other crew's rows. One overview plus ${lists} separately opened department lists.`,
      storage: "Three independent in-memory Jazz runtimes",
      includes: [
        "Query preparation and subscription admission",
        "Core, relay and foreground progress",
        "Initial result consumption",
      ],
      excludes: [
        "Seeding, database opening and connection setup",
        "Teardown, mutation correctness checks",
        "Serialization, network latency, IndexedDB and React",
      ],
      work: {
        count: lists + 1,
        unit: "subscriptions/s",
        explanation:
          "All initial settled subscriptions per measured dashboard opening; not row throughput.",
      },
      source,
    }),
  ),
  board(
    "stage_plan_update_under_policy[9000]",
    "memory",
    "StagePlan · update under a row-dependent policy",
    "Toggle an indexed field on one activity row under a trusted-session, row-dependent SELECT/UPDATE policy.",
    "3,000 tasks, 12,000 comments and 9,000 activity rows.",
    ["One policy-checked update and local settlement"],
    "permissioned updates/s",
    "One completed update per iteration.",
  ),
  board(
    "stage_plan_subscribe_under_policy[9000]",
    "memory",
    "StagePlan · open a live view under a row-dependent policy",
    "Attach one point activity subscription and consume its initial result under the row-dependent activity policy.",
    "300 tasks, 1,200 comments and 9,000 activity rows.",
    ["Subscription attachment and initial-event consumption"],
    "subscriptions attached/s",
    "One initial point subscription per iteration; fixture row count is load context.",
  ),
  board(
    "stage_plan_archive_task[9000]",
    "memory",
    "StagePlan · archive a task",
    "Delete (archive) one task and wait for local settlement in a freshly seeded fixture.",
    "300 tasks, 1,200 comments and 9,000 activity rows.",
    ["One task deletion and local settlement"],
    "archives/s",
    "One settled delete per iteration.",
  ),
  board(
    "stage_plan_restore_task[9000]",
    "memory",
    "StagePlan · restore an archived task",
    "Restore one previously archived task and wait for local settlement. The archiving is fixture setup.",
    "300 tasks, 1,200 comments and 9,000 activity rows.",
    ["One task restoration and local settlement"],
    "restores/s",
    "One settled restore per iteration.",
  ),
  board(
    "stage_plan_resume_after_offline[(500, 2000, 1500)]",
    "memory",
    "StagePlan · reconnect after one offline change",
    "Reconnect the prepared byte-wire topology after one task changed while offline and consume the catch-up result. This is not a standalone update benchmark.",
    "500 tasks, 2,000 comments and 1,500 activity rows; the prior subscription and topology are prepared outside timing.",
    [
      "Connection/resume exchange, encoded delivery and receiver application",
      "Catch-up event consumption",
    ],
    "reconnects/s",
    "One reconnect/catch-up per iteration. A single changed task does not imply a one-row wire response.",
  ),
];
