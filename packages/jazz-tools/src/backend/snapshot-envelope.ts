import type { DehydratedSnapshot } from "./ssr.js";

/**
 * A single prefetched query's rendered result, keyed for the client
 * orchestrator. Internal: the public snapshot is opaque, so this never
 * leaves the package.
 */
export type RowEntry = {
  key: string;
  result: unknown;
};

/**
 * A prefetched query carrying both forms: its rendered `result` (synchronous
 * paint) and its `bundle` (base64 CRDT wire bytes, for flash-free store
 * hydration). One per prefetched query, so a page that prefetches several
 * queries hydrates each.
 */
export type BundleEntry = RowEntry & {
  bundle: string;
};

/**
 * The swappable representation carried inside a snapshot. Both forms carry
 * `entries` (rendered rows, for synchronous first paint); the `bundle` form
 * additionally carries the server's own CRDT sync bundle (base64-encoded wire
 * bytes, for store hydration) so the live transition is flash-free. Only
 * `applySnapshot` dispatches on this — consumers never see it.
 */
export type SnapshotPayload =
  | { kind: "rows"; entries: RowEntry[] }
  | { kind: "bundle"; entries: BundleEntry[] };

/**
 * The internal wire shape behind the opaque {@link DehydratedSnapshot}. The
 * envelope metadata (appId/principalId/schemaFingerprint) is stable across
 * representations; only `payload` changes when the representation is swapped,
 * and `v` lets an older client recognise a payload it can't interpret.
 */
export type SnapshotEnvelope = {
  v: 1;
  appId: string;
  principalId: string | null;
  schemaFingerprint: string;
  payload: SnapshotPayload;
};

/** Seal an envelope as the opaque public snapshot (identity at runtime). */
export function sealSnapshot(envelope: SnapshotEnvelope): DehydratedSnapshot {
  return envelope as unknown as DehydratedSnapshot;
}

/** Recover the envelope inside the package boundary (identity at runtime). */
export function openSnapshot(snapshot: DehydratedSnapshot): SnapshotEnvelope {
  return snapshot as unknown as SnapshotEnvelope;
}
