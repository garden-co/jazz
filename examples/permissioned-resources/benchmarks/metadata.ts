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
      "Bring an empty client to a fully settled permissioned view straight from Core (workload revision 3). Core authorizes the fixed Member identity, not anonymous. Not comparable with revisions 1 and 2, which measured a relay and a server edge.",
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
  {
    name: "first_sync_local_relay_27518_file",
    title: "Permissioned first sync via device-local relay",
    description:
      "Bring an empty client to a fully settled permissioned view through Core → device-local native relay → Client (workload revision 3). The relay is fresh and empty; the client is its in-memory foreground, as on a device. Core authorizes the fixed Member identity, not anonymous. Not comparable with first_sync_local_relay_27518_rocksdb (revision 2).",
    fixture:
      "39 subscriptions; 27,518 visible resource, permission-input and inherited-access child rows. Shallow-history fixture, scale 1.",
    storage:
      "File-backed SQLite on Core and the native relay (WAL, synchronous=NORMAL), in-memory client; in-process encoded wire with a zstd roundtrip per frame between Core and the relay",
    includes: [
      "Relay opening, client attachment and connection",
      "Query preparation, subscription and settling until every expected row is present in the client",
      "Wire encoding, zstd roundtrip and decoding per frame; the relay's owner-thread handoffs",
    ],
    excludes: [
      "Core seeding",
      "Exact per-table row-ID verification",
      "Relay shutdown and real-network latency",
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
    name: "first_sync_bundle_27518_file",
    title: "Permissioned first sync from an initial bundle",
    description:
      "Bring an empty client to a fully settled permissioned view by starting it from a bundle (workload revision 3, experimental): Core writes the reader's replica for the 39 subscriptions as a ready-made SQLite file, the client opens it after a zstd roundtrip, connects and resumes its subscriptions from the bundle's seq. Core authorizes the fixed Member identity, not anonymous. Same cardinality as first_sync_direct_27518_file.",
    fixture:
      "39 subscriptions; 27,518 visible resource, permission-input and inherited-access child rows. Shallow-history fixture, scale 1.",
    storage:
      "File-backed SQLite on Core and the bundled client (the bundle file is the client's store); in-process encoded wire with a zstd roundtrip per frame",
    includes: [
      "Building the bundle on Core (policy-compiled reads into an attached file)",
      "Bundle zstd compression, decompression and write, and opening the client on it",
      "Connection, query preparation, subscription and settling until every subscription is settled with its expected rows",
    ],
    excludes: [
      "Core seeding",
      "Exact per-table row-ID verification",
      "Real-network latency and download time",
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
