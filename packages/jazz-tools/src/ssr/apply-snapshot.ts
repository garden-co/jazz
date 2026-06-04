import type { DehydratedSnapshot } from "../backend/ssr.js";
import type { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

export type ApplySnapshotExpected = {
  appId: string;
  principalId: string | null;
  schemaFingerprint: string;
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

  if (snapshot.appId !== expected.appId) {
    warnDiscard(
      "appId",
      `expected ${JSON.stringify(expected.appId)} but envelope had ${JSON.stringify(snapshot.appId)}`,
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
    snapshot.principalId !== null &&
    expected.principalId !== null &&
    snapshot.principalId !== expected.principalId
  ) {
    throw new Error(
      `[jazz] refusing to seed SSR snapshot: it is scoped to principal ${JSON.stringify(
        snapshot.principalId,
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
  if (snapshot.schemaFingerprint !== expected.schemaFingerprint) {
    warnDiscard(
      "schemaFingerprint",
      `expected ${JSON.stringify(expected.schemaFingerprint)} but envelope had ${JSON.stringify(snapshot.schemaFingerprint)}`,
    );
    return "schema-mismatch";
  }

  for (const entry of snapshot.entries) {
    manager.seedSnapshot(entry.key, entry.result as { id: string }[]);
  }

  return "applied";
}

function warnDiscard(field: string, detail: string): void {
  if (typeof console === "undefined" || !console.warn) return;
  console.warn(
    `[jazz] discarding SSR snapshot: ${field} mismatch — ${detail}. Falling back to a live fetch.`,
  );
}
