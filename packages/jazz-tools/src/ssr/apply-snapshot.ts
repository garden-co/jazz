import type { DehydratedSnapshot } from "../backend/ssr.js";
import { openSnapshot } from "../backend/snapshot-envelope.js";
import type { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

export type ApplySnapshotExpected = {
  /** Skip the appId check when undefined (the caller has nothing to compare against). */
  appId?: string;
  principalId: string | null;
  /** Skip the schema check when undefined (already opt-in via the provider's `schema` prop). */
  schemaFingerprint?: string;
};

export type ApplySnapshotInput = {
  manager: SubscriptionsOrchestrator;
  snapshot: DehydratedSnapshot | undefined;
  expected: ApplySnapshotExpected;
};

export type ApplySnapshotOutcome =
  | "applied"
  | "no-snapshot"
  | "post-attach"
  | "appId-mismatch"
  | "schema-mismatch";

export function applySnapshot({
  manager,
  snapshot,
  expected,
}: ApplySnapshotInput): ApplySnapshotOutcome {
  if (!snapshot) {
    return "no-snapshot";
  }

  // Once the live db has attached, it is authoritative: a late-mounting hook
  // (tab, modal, client-side nav) drops its frozen render-time snapshot rather
  // than seeding stale rows, and just subscribes to the live store.
  if (manager.isAttached()) {
    return "post-attach";
  }

  const env = openSnapshot(snapshot);

  if (expected.appId !== undefined && env.appId !== expected.appId) {
    warnDiscard(
      "appId",
      `expected ${JSON.stringify(expected.appId)} but envelope had ${JSON.stringify(env.appId)}`,
    );
    return "appId-mismatch";
  }

  // Seeding is never gated on the principal: the client trusts a server-rendered
  // snapshot, and a seed with no live principal yet still renders on first paint.
  // Keeping the snapshot scoped to the right viewer is the server's job.
  //
  // We do still throw when both principals are known and disagree — a snapshot
  // for one principal reaching a confirmed different live one is a misconfiguration
  // worth surfacing rather than silently showing another user's rows.
  if (
    env.principalId !== null &&
    expected.principalId !== null &&
    env.principalId !== expected.principalId
  ) {
    throw new Error(
      `[jazz] refusing to seed SSR snapshot: it is scoped to principal ${JSON.stringify(
        env.principalId,
      )} but the live session is ${JSON.stringify(
        expected.principalId,
      )} — seeding it would expose another principal's rows.`,
    );
  }

  // Seeding requires the client to be on the same schema as the server. On any
  // difference we discard and let live sync fill the data instead. Cross-version
  // lensing may come later but is likely to be relatively low-value: a client
  // ahead of the server is unlikely, so the realistic case is the server ahead
  // of the client — and then the client has to fetch the newer schema from the
  // sync server anyway, so there's nothing to gain over just waiting for sync.
  if (
    expected.schemaFingerprint !== undefined &&
    env.schemaFingerprint !== expected.schemaFingerprint
  ) {
    warnDiscard(
      "schemaFingerprint",
      `expected ${JSON.stringify(expected.schemaFingerprint)} but envelope had ${JSON.stringify(env.schemaFingerprint)}`,
    );
    return "schema-mismatch";
  }

  // The single point that knows the representation. Both forms seed the
  // rendered rows for synchronous first paint; the bundle form additionally
  // queues the server's CRDT bundle so the orchestrator can hydrate the store
  // (flash-free) the moment the db attaches. Consumers never see this.
  const { payload } = env;
  for (const entry of payload.entries) {
    manager.seedSnapshot(entry.key, entry.result as { id: string }[]);
  }
  if (payload.kind === "bundle") {
    for (const entry of payload.entries) {
      manager.queueBundle(decodeBase64(entry.bundle));
    }
  }

  return "applied";
}

/** Decode the base64 wire bytes carried in a bundle-form snapshot. */
function decodeBase64(base64: string): Uint8Array {
  if (typeof Buffer !== "undefined") {
    return new Uint8Array(Buffer.from(base64, "base64"));
  }
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function warnDiscard(field: string, detail: string): void {
  if (typeof console === "undefined" || !console.warn) return;
  console.warn(
    `[jazz] discarding SSR snapshot: ${field} mismatch — ${detail}. Falling back to a live fetch.`,
  );
}
