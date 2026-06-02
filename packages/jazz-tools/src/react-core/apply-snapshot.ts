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

export type ApplySnapshotOutcome =
  | "applied"
  | "no-snapshot"
  | "appId-mismatch"
  | "principal-mismatch"
  | "schema-mismatch";

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

  // A null principalId marks a public snapshot — prefetched without user
  // scoping — so it seeds into any session. Only user-scoped (non-null)
  // snapshots must match the live principal.
  if (snapshot.principalId !== null && snapshot.principalId !== expected.principalId) {
    warnDiscard(
      "principalId",
      `expected ${JSON.stringify(expected.principalId)} but envelope had ${JSON.stringify(snapshot.principalId)}`,
    );
    return "principal-mismatch";
  }

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
