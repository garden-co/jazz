import type { PublicSession } from "../runtime/context.js";
import { getDbSubscriptionSource, type Db, type ShutdownOptions } from "../runtime/db.js";
import { runCleanupSteps } from "../runtime/run-cleanup-steps.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import type { DbConfig } from "./create-db.js";
import { createAccountDbWithRuntimeSource, type AccountDbConfig } from "../accounts/context.js";
import { ReactNativeRuntimeSource } from "./runtime-source.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";

export interface JazzClient {
  db: Db;
  session: PublicSession | null;
  shutdown(options?: ShutdownOptions): Promise<void>;
}

/**
 * The only React-Native client configuration surface. In persistent mode its
 * optional `nativeRelay` field carries the opaque capability issued by the
 * application's trusted native admission code; no JSI factory, byte codec,
 * storage path, or native owner helper is part of this public API.
 */
export type JazzClientConfig = AccountDbConfig & Pick<DbConfig, "nativeRelay" | "sqliteStorage">;

async function createJazzClientInternal(config: JazzClientConfig): Promise<JazzClient> {
  const db = await createAccountDbWithRuntimeSource(config, new ReactNativeRuntimeSource());
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
      async shutdown(options?: ShutdownOptions) {
        if (options?.waitForSync) await db.shutdown(options);
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
