import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

export const permissionedBenchmarks: BenchmarkMetadata[] = [
  {
    name: "first_sync_local_relay_27518_rocksdb",
    title: "Permissioned first sync via device-local relay",
    description:
      "Bring an empty client to a fully settled permissioned view through Core → device-local persistence relay → Client. Core authorizes the fixed Member identity (not anonymous); both device hops use that identity.",
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
    name: "first_sync_direct_27518_file",
    title: "Permissioned first sync",
    description:
      "Bring an empty client to a fully settled permissioned view straight from Core (workload revision 3). Core authorizes the fixed Member identity, not anonymous. Not comparable with the relay or server-edge topologies, which the SQLite engine does not have.",
    fixture:
      "39 subscriptions; 27,518 visible resource, permission-input and inherited-access child rows. Shallow-history fixture, scale 1.",
    storage:
      "File-backed SQLite on Core and Client (WAL, synchronous=NORMAL); in-process encoded wire with a zstd roundtrip per frame",
    includes: [
      "Client opening and connection",
      "Query preparation, subscription and settling until every expected row is present",
      "Wire encoding, zstd roundtrip and decoding per frame",
    ],
    excludes: [
      "Core seeding",
      "Exact per-table row-ID verification",
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
