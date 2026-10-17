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
];
