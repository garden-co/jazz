import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const common = {
  storage: "RocksDB worker + in-memory foreground; WAL without fsync",
  source: "examples/todo-client-localfirst-ts/benchmarks/benches/walltime.rs",
  excludes: [
    "Fixture seeding",
    "Exact row-ID correctness verification",
    "Browser/JS scheduling, authentication bootstrap and real-network latency",
  ],
};
export const todoBenchmarks: BenchmarkMetadata[] = [
  {
    ...common,
    name: "sequential_insert_1350_rocksdb",
    title: "Sequential todo inserts",
    description:
      "Insert 1,350 tasks in separate transactions, completing foreground-to-worker-to-foreground delivery before starting each next insert.",
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
    ...common,
    name: "sequential_update_1350_rocksdb",
    title: "Sequential todo updates",
    description:
      "Mark 1,350 tasks done in separate transactions, with worker roundtrip and foreground delivery after each update.",
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
    ...common,
    name: "batch_update_1350_rocksdb",
    title: "Batched todo updates",
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
    ...common,
    name: "reopen_1500_rocksdb",
    title: "Todo reopen and first read",
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
];
