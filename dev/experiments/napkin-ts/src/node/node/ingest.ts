// Commit, fate, and sync-message ingestion for a storage-backed node: the node layer's write side.
import {
  nodeError,
  BranchKey,
  type CommitUnitIngestContext,
  type DurabilityTier,
  type Fate,
  type Transaction,
  type VersionRecord,
} from "../../_extern/node-physical-ingest.js";
import type { ParkedIngressRole } from "./codec.js";

/**
 * How long a write chained on a predecessor parked at the authority waits behind it, measured on the
 * authority's ingest clock (the newest `now_ms` of any commit ingest).
 */
export const PARKED_PREDECESSOR_WAIT_MS = 60_000;

export const MAX_SCHEMA_LINEAGE_DECLARATIONS = 4096;
export const MAX_SCHEMA_LINEAGE_NAME_BYTES = 1024;
export const MAX_SCHEMA_LINEAGE_OPS = 16_384;

export function authorityWallClockMs(): number {
  const now = Date.now();
  if (now < 0) throw nodeError("InvalidStoredValue", "authority clock precedes Unix epoch");
  if (!Number.isSafeInteger(now))
    throw nodeError("InvalidStoredValue", "authority clock exceeds u64 milliseconds");
  return now;
}

export type CommitUnitParkMode = {
  ingestContext: CommitUnitIngestContext | undefined;
  ingressRole: ParkedIngressRole;
};
export const defaultCommitUnitParkMode = (): CommitUnitParkMode => ({
  ingestContext: undefined,
  ingressRole: "Authority",
});

export * from "./ingest/catalogue.js";
export * from "./ingest/commit_bundles.js";
export * from "./ingest/fates.js";
export * from "./ingest/view_updates.js";
export * from "./ingest/validation.js";
export * from "./ingest/write_base.js";

/** A sequence is the global-authority receipt: peer payloads pairing it with weaker durability are rejected. */
export function validateReceivedFateUpdateGlobalTimeDurability(
  globalTime: bigint | undefined,
  durability: DurabilityTier | undefined,
): void {
  if (globalTime !== undefined && durability !== "Global")
    throw nodeError("UnsupportedSyncMessage", "global timestamp requires Global durability");
}

/** View bundles are peer payloads too, including reset bundles eligible for bulk persistence. */
export function validateReceivedViewBundleGlobalTimeDurability(
  globalTime: bigint | undefined,
  durability: DurabilityTier,
): void {
  if (globalTime !== undefined && durability !== "Global")
    throw nodeError("MalformedViewUpdate", "global timestamp requires Global durability");
}

export const fateUpdateDurabilityClaim = (
  fate: Fate,
  durability: DurabilityTier,
): DurabilityTier | undefined => (fate.kind === "Rejected" ? undefined : durability);

export const commitUnitWriteCountMatches = (tx: Transaction, versionCount: number): boolean =>
  Number(tx.nTotalWrites) === versionCount;

/** One transaction carries at most one version per row, so a row names its version within the transaction. */
export const viewVersionKeyForIngest = (version: VersionRecord): string =>
  `${version.table()}\0${BranchKey.key(version.branchKey())}\0${Array.from(version.rowUuid() as Uint8Array).join(",")}`;
