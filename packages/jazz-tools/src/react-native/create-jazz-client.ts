import type { Db, DbConfig } from "./db.js";
import { createDb } from "./db.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";

export interface JazzClient {
  db: Db;
  manager: SubscriptionsOrchestrator;
  shutdown(): Promise<void>;
}

async function createJazzClientInternal(config: DbConfig): Promise<JazzClient> {
  const db = await createDb(config);
  const manager = new SubscriptionsOrchestrator({ appId: config.appId }, db);

  await manager.init();

  return {
    db,
    manager,
    async shutdown() {
      await manager.shutdown();
      await db.shutdown();
    },
  };
}

export function createJazzClient(config: DbConfig): Promise<JazzClient> {
  return trackPromise(createJazzClientInternal(config));
}
