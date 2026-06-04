import type { Session } from "../runtime/context.js";
import { acquireClient, releaseClient } from "../runtime/client-registry.js";
import type { Db, DbConfig } from "../runtime/db.js";
import { createDb } from "../runtime/db.js";
import { createDbFromInspectedPage } from "../dev-tools/index.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { registerWindowJazzStorageClient } from "../window-client-storage.js";

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
  const key = JSON.stringify(config);
  const holder = {};
  const shared = acquireClient(key, () => createJazzClientInternal(config), holder);
  return trackPromise(
    shared.then((client) => ({
      db: client.db,
      get session() {
        return client.session;
      },
      manager: client.manager,
      shutdown() {
        return releaseClient(key, holder);
      },
    })),
  );
}

async function createExtensionJazzClientInternal(): Promise<JazzClient> {
  const db = await createDbFromInspectedPage();
  const config = db.getConfig();
  if (!config) {
    throw new Error("DevTools bridge did not provide an inspected page config.");
  }
  let session = db.getAuthState().session;
  const manager = new SubscriptionsOrchestrator({ appId: config.appId }, db);
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

export function createExtensionJazzClient(): Promise<JazzClient> {
  return trackPromise(createExtensionJazzClientInternal());
}
