import type { DehydratedSnapshot } from "../backend/ssr.js";
import { openSnapshot } from "../backend/snapshot-envelope.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { applySnapshot } from "./apply-snapshot.js";

// A db that never delivers, for the seed-only orchestrator: seeded entries are
// already fulfilled from the snapshot, and there's no live connection to stream
// updates until the real client swaps in.
const NOOP_SEED_DB = {
  subscribeAll(): () => void {
    return () => {};
  },
} as ConstructorParameters<typeof SubscriptionsOrchestrator>[1];

export type SeedOptions = {
  /** When set, the snapshot is only seeded if its fingerprint matches. */
  schemaFingerprint?: string;
};

/**
 * Build a read-only {@link SubscriptionsOrchestrator} seeded synchronously from
 * a server-rendered snapshot. Used by framework providers to render the
 * prefetched rows on the first paint — on the server and the client — before
 * the live client connects.
 *
 * The orchestrator is keyed to the snapshot's own appId so the seeded entries
 * line up with the first-paint `useAll` lookups — which makes an appId check
 * tautological here, so only the optional schema check is honoured. The seed
 * runs with a `null` live principal, which under the trust-the-server model in
 * {@link applySnapshot} seeds the snapshot's rows regardless of their principal.
 * Keeping the snapshot scoped to the right viewer is the server's job.
 */
export function createSeedOrchestrator(
  snapshot: DehydratedSnapshot,
  options: SeedOptions = {},
): SubscriptionsOrchestrator {
  const appId = openSnapshot(snapshot).appId;
  const manager = new SubscriptionsOrchestrator({ appId }, NOOP_SEED_DB);
  applySnapshot({
    manager,
    snapshot,
    expected: {
      principalId: null,
      schemaFingerprint: options.schemaFingerprint,
    },
  });
  return manager;
}

/**
 * Build an empty, db-less {@link SubscriptionsOrchestrator} for the per-hook
 * seed phase: the hooks themselves seed it via `useAll(query, { snapshot })`,
 * and the provider points it at the live db once connected. Its appId is taken
 * from the snapshot so the keys line up with the seeded entries.
 */
export function createDbLessOrchestrator(snapshot: DehydratedSnapshot): SubscriptionsOrchestrator {
  return new SubscriptionsOrchestrator({ appId: openSnapshot(snapshot).appId }, NOOP_SEED_DB);
}
