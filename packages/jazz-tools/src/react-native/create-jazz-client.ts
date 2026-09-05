import type { PublicSession } from "../runtime/context.js";
import { getDbSubscriptionSource, type Db } from "../runtime/db.js";
import { runCleanupSteps } from "../runtime/run-cleanup-steps.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import { createDb, type DbConfig } from "./create-db.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";

export interface JazzClient {
  db: Db;
  session: PublicSession | null;
  shutdown(): Promise<void>;
}

/**
 * The only React-Native client configuration surface. In persistent mode its
 * optional `nativeRelay` field carries the opaque capability issued by the
 * application's trusted native admission code; no JSI factory, byte codec,
 * storage path, or native owner helper is part of this public API.
 */
export type JazzClientConfig = DbConfig;

async function createJazzClientInternal(config: JazzClientConfig): Promise<JazzClient> {
  const db = await createDb(config);
  let session = db.getAuthState().session;
  const manager = new SubscriptionsOrchestrator(
    { appId: config.appId },
    getDbSubscriptionSource(db),
    getDbInternalSession(db),
  );
  await manager.init();
  const stopSessionSync = db.onAuthChanged(({ session: nextSession }) => {
    session = nextSession ?? null;
    manager.setSession(getDbInternalSession(db));
  });

  return attachSubscriptionStore(
    {
      db,
      get session() {
        return session;
      },
      async shutdown() {
        await runCleanupSteps([
          () => stopSessionSync?.(),
          () => manager.shutdown(),
          () => db.shutdown(),
        ]);
      },
    },
    manager,
  );
}

export function createJazzClient(config: JazzClientConfig): Promise<JazzClient> {
  return trackPromise(createJazzClientInternal(config));
}
