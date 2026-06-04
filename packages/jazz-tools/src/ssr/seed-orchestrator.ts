import type { DehydratedSnapshot } from "../backend/ssr.js";
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

export type SeedExpected = {
  appId: string;
  schemaFingerprint: string;
};

/**
 * Build a read-only {@link SubscriptionsOrchestrator} seeded synchronously from
 * a server-rendered snapshot. Used by framework providers to render the
 * prefetched rows on the first paint — on the server and the client — before
 * the live client connects.
 *
 * The seed runs with a `null` live principal, which under the trust-the-server
 * model in {@link applySnapshot} seeds the snapshot's rows regardless of their
 * principal — the client trusts what the server rendered. Keeping the snapshot
 * scoped to the right viewer is the server's responsibility.
 */
export function createSeedOrchestrator(
  snapshot: DehydratedSnapshot,
  expected: SeedExpected,
): SubscriptionsOrchestrator {
  const manager = new SubscriptionsOrchestrator({ appId: expected.appId }, NOOP_SEED_DB);
  applySnapshot({
    manager,
    snapshot,
    expected: {
      appId: expected.appId,
      principalId: null,
      schemaFingerprint: expected.schemaFingerprint,
    },
  });
  return manager;
}
