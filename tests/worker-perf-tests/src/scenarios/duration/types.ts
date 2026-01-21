import type { RawCoID } from "cojson";
import type { MixSpec, OpKind } from "../../utils/mix.ts";

export type DurationWorkerData = {
  workerId: number;
  peer: string;
  durationMs: number;
  inflight: number;
  mixSpec: MixSpec;
  seed: number;
  targets: {
    fileIds: string[];
    mapIds: string[];
  };
};

export type DurationWorkerStats = {
  type: "stats";
  workerId: number;
  opsDone: number;
  fileOpsDone: number;
  fullFileOpsDone: number;
  mapOpsDone: number;
  unavailable: number;
};

export type DurationSeedResult = {
  db: string;
  scenario: "duration";
  configId: RawCoID;
  groupId: RawCoID;
  fileIds: RawCoID[];
  mapIds: RawCoID[];
  pdfName: string;
  pdfBytes: number;
};

export type { MixSpec, OpKind };
