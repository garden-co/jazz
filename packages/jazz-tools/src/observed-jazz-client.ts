import type { PublicSession } from "./runtime/context.js";
import { getDbSubscriptionSource, type Db, type ShutdownOptions } from "./runtime/db.js";
import { getDbInternalSession } from "./runtime/db-internal-session.js";
import { runCleanupSteps } from "./runtime/run-cleanup-steps.js";
import { SubscriptionsOrchestrator } from "./subscriptions-orchestrator.js";
import { attachSubscriptionStore } from "./subscription-store-internal.js";

export interface JazzClient {
  db: Db;
  session: PublicSession | null;
  shutdown(options?: ShutdownOptions): Promise<void>;
}

/**
 * Wrap an admitted Db for framework observers: track its live session, keep a
 * subscription orchestrator in step with it, and shut both down with the Db.
 * `attach` runs once the session is tracked; its cleanup runs on shutdown
 * right after session tracking stops.
 */
export async function createObservedJazzClient(
  db: Db,
  appId: string,
  attach?: (db: Db) => () => void,
): Promise<JazzClient> {
  let session = db.getAuthState().session;
  const manager = new SubscriptionsOrchestrator(
    { appId },
    getDbSubscriptionSource(db),
    getDbInternalSession(db),
  );
  await manager.init();
  const stopSessionSync = db.onAuthChanged(({ session: nextSession }) => {
    session = nextSession ?? null;
    manager.setSession(getDbInternalSession(db));
  });
  const detach = attach?.(db);

  return attachSubscriptionStore(
    {
      db,
      get session() {
        return session;
      },
      async shutdown(options?: ShutdownOptions) {
        if (options?.waitForSync) await db.shutdown(options);
        await runCleanupSteps([
          () => stopSessionSync?.(),
          ...(detach ? [detach] : []),
          () => manager.shutdown(),
          () => db.shutdown(),
        ]);
      },
    },
    manager,
  );
}
