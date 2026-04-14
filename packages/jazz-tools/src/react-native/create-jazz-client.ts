import type { Db, DbConfig } from "./db.js";
import { createDb } from "./db.js";
import type { Session } from "../runtime/context.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";

export interface JazzClient {
  db: Db;
  session: Session | null;
  manager: SubscriptionsOrchestrator;
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

  return {
    db,
    get session() {
      return session;
    },
    manager,
    async shutdown() {
      stopSessionSync?.();
      await manager.shutdown();
      await db.shutdown();
    },
  };
}

export function createJazzClient(config: DbConfig): Promise<JazzClient> {
  return trackPromise(createJazzClientInternal(config));
}
