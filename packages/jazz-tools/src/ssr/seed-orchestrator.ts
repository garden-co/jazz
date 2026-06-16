import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

// A db that never delivers, for the seed-phase orchestrator. Its entries are
// already filled from the snapshot, and there's nothing to stream until the live
// db attaches.
const NOOP_SEED_DB = {
  subscribeAll(): () => void {
    return () => {};
  },
} as ConstructorParameters<typeof SubscriptionsOrchestrator>[1];

/**
 * Build an empty orchestrator (no db yet) for the seed phase. Each
 * `useAll(query, { snapshot })` seeds it, and the provider attaches the live db
 * once connected. It's keyed to the app's appId — the same one the live client
 * uses — so the cache keys still match after the live db attaches.
 */
export function createDbLessOrchestrator(appId: string): SubscriptionsOrchestrator {
  return new SubscriptionsOrchestrator({ appId }, NOOP_SEED_DB);
}
