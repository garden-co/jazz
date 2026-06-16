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

export type ApplySnapshotOutcome = "applied" | "no-snapshot" | "appId-mismatch" | "schema-mismatch";

export function applySnapshot({
  manager,
  snapshot,
  expected,
}: ApplySnapshotInput): ApplySnapshotOutcome {
  if (!snapshot) {
    return "no-snapshot";
  }

  const env = openSnapshot(snapshot);

  if (expected.appId !== undefined && env.appId !== expected.appId) {
    warnDiscard(
      "appId",
      `expected ${JSON.stringify(expected.appId)} but envelope had ${JSON.stringify(env.appId)}`,
    );
    return "appId-mismatch";
  }

  // The client trusts a server-rendered snapshot and displays it: seeding is
  // never gated on the principal. A snapshot with no live principal yet (the
  // synchronous SSR seed) still seeds, so its rows render on the first paint.
  // Keeping the snapshot scoped to the right viewer is the server's job —
  // prefetch with the right Db, and don't serve one principal's render to
  // another; the rendered HTML already carries that data regardless.
  //
  // We do still throw when *both* principals are known and disagree: a snapshot
  // scoped to one principal reaching a confirmed different live principal (the
  // live-swap, once the client's session resolves) is a misconfiguration worth
  // surfacing loudly rather than silently swapping in another user's rows.
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

  // SSR snapshot seeding currently requires the client to be on the *same*
  // schema as the server that produced the snapshot. On any schema difference
  // we discard and fall back to a live fetch — so a client on a different build
  // than the server gets no first-paint seeding. Cross-version reinterpretation
  // (apply the client's lenses to the snapshot, delegated to the core, falling
  // back to live only when the lens chain is unavailable) is future work:
  // specs/todo/ideas/3_later/ssr-snapshot-cross-version-lensing.md
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
