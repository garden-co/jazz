import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

export const permissionedBenchmarks: BenchmarkMetadata[] = [
  {
    name: "first_sync_27518_rocksdb",
    title: "Permissioned first sync (retired server-edge topology)",
    description:
      "Bring an empty client to a fully settled permissioned view through Core → Edge → Client. The fixed identity is Member, not anonymous.",
    fixture:
      "39 subscriptions; 27,518 visible resource, permission-input and inherited-access child rows. Shallow-history fixture, scale 1.",
    storage: "RocksDB on Core, Edge and Client; in-process encoded/compressed transport",
    includes: [
      "Receiver opening and connection",
      "Query preparation, subscription and settling until every expected row is present",
      "Native encoding, zstd roundtrip and decoding per delivery",
    ],
    excludes: [
      "Core seeding",
      "Exact per-table row-ID verification and diagnostic scans",
      "Runtime teardown and real-network latency",
    ],
    work: {
      count: 27518,
      unit: "visible rows/s",
      explanation:
        "27,518 visible result rows per complete first sync. This is not wire-row throughput or raw database scan speed.",
    },
    source: "examples/permissioned-resources/benchmarks/benches/walltime.rs",
  },
  {
    name: "first_sync_local_relay_27518_rocksdb",
    title: "Permissioned first sync via device-local relay",
    description:
      "Bring an empty client to a fully settled permissioned view through Core → device-local persistence relay → Client. Core authorizes the fixed Member identity (not anonymous); both device hops use that identity. Workload revision 2 is a new baseline, not directly comparable with the retired server-edge topology.",
    fixture:
      "39 subscriptions; 27,518 visible resource, permission-input and inherited-access child rows. Shallow-history fixture, scale 1.",
    storage: "RocksDB on Core, local relay and Client; in-process encoded/compressed transport",
    includes: [
      "Receiver opening and connection",
      "Query preparation, subscription and settling until every expected row is present",
      "Native encoding, zstd roundtrip and decoding per delivery",
    ],
    excludes: [
      "Core seeding",
      "Exact per-table row-ID verification and diagnostic scans",
      "Runtime teardown and real-network latency",
    ],
    work: {
      count: 27518,
      unit: "visible rows/s",
      explanation:
        "27,518 visible result rows per complete first sync. This is not wire-row throughput or raw database scan speed.",
    },
    source: "examples/permissioned-resources/benchmarks/benches/walltime.rs",
  },
  {
    name: "initial_selects_39_tables_27518_rows_rocksdb",
    title: "Initial permissioned SELECTs across 39 tables",
    description:
      "Execute one unbounded SELECT per table on a fresh reopened authority for the fixed Member identity. Each sample owns a new runtime; warm query-plan reuse across samples is forbidden.",
    fixture:
      "39 queries returning 27,518 rows. Same anonymized shallow-history resource, recursive permission and inherited-access fixture as first sync; no deleted rows.",
    storage: "Fresh RocksDB copy reopened with the seeded node identity; OS cache may be warm",
    includes: [
      "First execution, cold query lowering and permission evaluation for each prepared query",
      "Storage scans, row normalization and collecting all returned rows",
    ],
    excludes: [
      "Schema/fixture creation, store copy/open and query preparation",
      "Independent exact UUID-order checks and encoded-result receipts",
      "Returned-row destruction, database close and fixture teardown",
      "Subscriptions, synchronization and copied-store recovery under a different node identity",
    ],
    work: {
      count: 27518,
      unit: "visible rows/s",
      explanation: "27,518 rows from one fresh-runtime sweep of 39 unbounded SELECTs.",
    },
    source: "examples/permissioned-resources/benchmarks/benches/walltime.rs",
  },
  {
    name: "initial_selects_39_tables_limit100_879_rows_rocksdb",
    title: "Initial permissioned 100-row pages across 39 tables",
    description:
      "Execute one SELECT with LIMIT 100 per table on a fresh reopened authority for the fixed Member identity. The page is the exact authorized UUID-ordered prefix.",
    fixture:
      "39 queries returning 879 rows in total from the full-scale permissioned-resource fixture. Small tables contribute fewer than 100 rows; large tables exercise bounded page reads.",
    storage: "Fresh RocksDB copy reopened with the seeded node identity; OS cache may be warm",
    includes: [
      "First execution, cold query lowering and permission evaluation for each prepared query",
      "Bounded candidate reads, row normalization and collecting all returned pages",
    ],
    excludes: [
      "Schema/fixture creation, store copy/open and query preparation",
      "Independent exact UUID-order checks and encoded-result receipts",
      "Returned-row destruction, database close and fixture teardown",
      "Subscriptions, synchronization and copied-store recovery under a different node identity",
    ],
    work: {
      count: 879,
      unit: "visible rows/s",
      explanation: "879 rows from one fresh-runtime sweep of 39 SELECTs, each limited to 100 rows.",
    },
    source: "examples/permissioned-resources/benchmarks/benches/walltime.rs",
  },
];
