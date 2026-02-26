import type { Session } from "../runtime/context.js";
import { resolveClientSession } from "../runtime/client-session.js";
import { resolveLocalAuthDefaults } from "../runtime/local-auth.js";
import type { Db, DbConfig } from "../runtime/db.js";
import { createDb } from "../runtime/db.js";

export interface JazzClient {
  db: Db;
  session: Session | null;
  shutdown(): Promise<void>;
}

export async function createJazzClient(config: DbConfig): Promise<JazzClient> {
  const resolvedConfig = resolveLocalAuthDefaults(config);
  const [db, session] = await Promise.all([
    createDb(resolvedConfig),
    resolveClientSession(resolvedConfig),
  ]);

  return {
    db,
    session,
    async shutdown() {
      await db.shutdown();
    },
  };
}
