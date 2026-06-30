import type { Session } from "../runtime/context.js";
import type { Db, DbConfig } from "../runtime/db.js";
import { createDb } from "../runtime/db.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { registerWindowJazzStorageClient } from "../window-client-storage.js";

/**
 * @todo Make VUE, React and SVELTE consume from this agnostic client
 *
 * The following files are practically 1:1 to this one:
 * ./packages/jazz-tools/src/react/create-jazz-client.ts
 * ./packages/jazz-tools/src/vue/create-jazz-client.ts
 * ./packages/jazz-tools/src/svelte/create-jazz-client.ts
 **/

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
  const unregisterWindowJazzStorageClient = registerWindowJazzStorageClient(db);

  return {
    db,
    get session() {
      return session;
    },
    manager,
    async shutdown() {
      stopSessionSync?.();
      unregisterWindowJazzStorageClient();
      await manager.shutdown();
      await db.shutdown();
    },
  };
}

export function createJazzClient(config: DbConfig): Promise<JazzClient> {
  return trackPromise(createJazzClientInternal(config));
}
