import type { Session } from "../runtime/context.js";
import { resolveClientSession } from "../runtime/client-session.js";
import { resolveLocalAuthDefaults } from "../runtime/local-auth.js";
import type { Db, DbConfig } from "../runtime/db.js";
import { createDb } from "../runtime/db.js";
import { createDbFromInspectedPage } from "../dev-tools/index.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";

export interface JazzClient {
  db: Db;
  session: Session | null;
  manager: SubscriptionsOrchestrator;
  shutdown(): Promise<void>;
}

async function createJazzClientInternal(config: DbConfig): Promise<JazzClient> {
  const resolvedConfig = resolveLocalAuthDefaults(config);
  const [db, session] = await Promise.all([
    createDb(resolvedConfig),
    resolveClientSession(resolvedConfig),
  ]);

  const manager = new SubscriptionsOrchestrator({ appId: resolvedConfig.appId }, db, session);
  await manager.init();

  return {
    db,
    session,
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

async function createExtensionJazzClientInternal(): Promise<JazzClient> {
  const db = await createDbFromInspectedPage();
  const config = db.getConfig();
  if (!config) {
    throw new Error("DevTools bridge did not provide an inspected page config.");
  }

  const manager = new SubscriptionsOrchestrator({ appId: config.appId }, db);
  await manager.init();

  return {
    db,
    session: null,
    manager,
    async shutdown() {
      await manager.shutdown();
      await db.shutdown();
    },
  };
}

export function createExtensionJazzClient(): Promise<JazzClient> {
  return trackPromise(createExtensionJazzClientInternal());
}
