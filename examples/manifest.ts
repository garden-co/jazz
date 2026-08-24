/**
 * Public index for the examples-and-benchmarks gallery.
 *
 * Keep this deliberately data-only. App-local contracts own fixture and
 * scenario semantics; docs/gallery code can consume this list without
 * importing a product application's runtime.
 */
export type ExampleManifestEntry = {
  id: string;
  name: string;
  path: string;
  status: "in-progress" | "planned";
  capabilities: readonly string[];
  scenarioIds: readonly string[];
};

export const examplesAndBenchmarks: readonly ExampleManifestEntry[] = [
  {
    id: "band-chat",
    name: "BandChat",
    path: "examples/band-chat",
    status: "in-progress",
    capabilities: ["identity", "room permissions", "attachments", "offline reconnect"],
    scenarioIds: ["band-chat.topology.room-recovery"],
  },
  {
    id: "world-tour",
    name: "WorldTour",
    path: "examples/world-tour",
    status: "planned",
    capabilities: ["relations", "shared planning"],
    scenarioIds: [],
  },
  {
    id: "record-player",
    name: "RecordPlayer",
    path: "examples/record-player",
    status: "planned",
    capabilities: ["large values", "streams"],
    scenarioIds: [],
  },
  {
    id: "jamazon",
    name: "Jamazon",
    path: "examples/jamazon",
    status: "planned",
    capabilities: ["offline cart", "durable effects"],
    scenarioIds: [],
  },
  {
    id: "jamazon-warehouse",
    name: "Jamazon Warehouse",
    path: "examples/jamazon-warehouse",
    status: "planned",
    capabilities: ["transactions", "contention"],
    scenarioIds: [],
  },
  {
    id: "band-binder",
    name: "BandBinder",
    path: "examples/band-binder",
    status: "planned",
    capabilities: ["branches", "permissions"],
    scenarioIds: [],
  },
  {
    id: "wequencer",
    name: "Wequencer",
    path: "examples/wequencer",
    status: "planned",
    capabilities: ["high-frequency collaboration"],
    scenarioIds: [],
  },
  {
    id: "poster-shop",
    name: "PosterShop",
    path: "examples/poster-shop",
    status: "planned",
    capabilities: ["canvas", "history"],
    scenarioIds: [],
  },
  {
    id: "big-label",
    name: "BigLabel",
    path: "examples/big-label",
    status: "planned",
    capabilities: ["multi-tenancy", "policy graphs"],
    scenarioIds: [],
  },
  {
    id: "music-agent",
    name: "MusicAgent",
    path: "examples/music-agent",
    status: "in-progress",
    capabilities: ["streamed transcripts", "tool calls", "attachments", "durable execution"],
    scenarioIds: [],
  },
  {
    id: "epic-drop",
    name: "EpicDrop",
    path: "examples/epic-drop",
    status: "planned",
    capabilities: ["large values", "partial residency", "filesystem integration"],
    scenarioIds: [],
  },
];
