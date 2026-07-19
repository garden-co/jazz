import type { Session } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import { createDb, type DbConfig } from "./create-db.js";

export interface JazzClient {
  db: Db;
  session: Session | null;
  shutdown(): Promise<void>;
}

async function createJazzClientInternal(config: DbConfig): Promise<JazzClient> {
  const db = await createDb(config);
  let session = db.getAuthState().session;
  const manager = new SubscriptionsOrchestrator({ appId: config.appId }, db, session);
  await manager.init();
  const stopSessionSync = db.onAuthChanged(({ session: nextSession }) => {
    session = nextSession ?? null;
    manager.setSession(nextSession ?? null);
  });

  return attachSubscriptionStore(
    {
      db,
      get session() {
        return session;
      },
      async shutdown() {
        stopSessionSync?.();
        await manager.shutdown();
        await db.shutdown();
      },
    },
    manager,
  );
}

export function createJazzClient(config: DbConfig): Promise<JazzClient> {
  return trackPromise(createJazzClientInternal(config));
}
