import type { RawCoID } from "cojson";

export type BatchWorkerData = {
  workerId: number;
  peer: string;
  mapIds: string[];
  runIndex: number;
};

export type BatchWorkerResult = {
  type: "result";
  workerId: number;
  runIndex: number;
  totalTimeMs: number;
  latencies: number[];
  mapsLoaded: number;
  errors: number;
};

export type BatchSeedResult = {
  db: string;
  scenario: "batch";
  configId: RawCoID;
  groupId: RawCoID;
  mapIds: RawCoID[];
  minSize: number;
  maxSize: number;
};
