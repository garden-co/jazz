import type { RawCoID } from "cojson";

/**
 * Schema for seed configuration stored as a CoValue.
 * This is stored in a CoMap and loaded by tests to get the IDs to use.
 */

export type ScenarioType = "duration" | "batch";

/**
 * Base seed configuration shared by all scenarios.
 */
export type BaseSeedConfig = {
  scenario: ScenarioType;
  createdAt: number;
  groupId: RawCoID;
};

/**
 * Duration scenario configuration.
 * Used for loading files and maps concurrently for X seconds.
 */
export type DurationSeedConfig = BaseSeedConfig & {
  scenario: "duration";
  fileIds: RawCoID[];
  mapIds: RawCoID[];
  pdfName: string;
  pdfBytes: number;
};

/**
 * Batch benchmark scenario configuration.
 * Used for loading a set of maps and calculating percentiles.
 */
export type BatchSeedConfig = BaseSeedConfig & {
  scenario: "batch";
  mapIds: RawCoID[];
  minSize: number;
  maxSize: number;
};

export type SeedConfig = DurationSeedConfig | BatchSeedConfig;

/**
 * Keys used in the seed config CoMap.
 * Note: FILE_IDS and MAP_IDS store references to CoLists (RawCoID),
 * not JSON-stringified arrays.
 */
export const SEED_CONFIG_KEYS = {
  SCENARIO: "seed:scenario",
  CREATED_AT: "seed:createdAt",
  GROUP_ID: "seed:groupId",
  // Duration scenario - FILE_IDS and MAP_IDS are CoList references
  FILE_IDS: "seed:fileIds",
  MAP_IDS: "seed:mapIds",
  PDF_NAME: "seed:pdfName",
  PDF_BYTES: "seed:pdfBytes",
  // Batch scenario
  MIN_SIZE: "seed:minSize",
  MAX_SIZE: "seed:maxSize",
} as const;
