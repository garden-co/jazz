import { useEffect, type ReactNode } from "react";
import type { Session } from "../runtime/context.js";
import type { Db, DbConfig } from "../runtime/db.js";
import type { WasmSchema } from "../index.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
} from "../react-core/provider.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";

// In dev builds, pull in a generated module that withJazz (next.ts/vite.ts/...)
// rewrites on every schema push. The bundler tracks this as a dependency of the
// React provider, so any push to the file forces a full reload of the host app
// without each framework plugin needing its own dev-server WebSocket wiring.
if (process.env.NODE_ENV === "development" && typeof window !== "undefined") {
  import("jazz-tools/_dev/schema-hash").catch(() => {});
}

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  manager: CreatedJazzClient["manager"];
  session: Session | null;
  shutdown: CreatedJazzClient["shutdown"];
}

// Tracks db instances that already have devtools attached, so a manual
// attachDevTools call elsewhere doesn't double-attach via the provider.
const autoAttachedDbs = new WeakSet<object>();

function DevToolsAutoAttach({ wasmSchema }: { wasmSchema?: WasmSchema }) {
  const { db } = useCoreJazzClient() as JazzClientContextValue;
  useEffect(() => {
    if (!wasmSchema || autoAttachedDbs.has(db as object)) return;
    autoAttachedDbs.add(db as object);
    void import("../dev-tools/dev-tools.js").then(({ attachDevTools }) =>
      attachDevTools({ db }, wasmSchema),
    );
  }, [db, wasmSchema]);
  return null;
}

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
  autoAttachDevTools?: boolean;
  wasmSchema?: WasmSchema;
};

export function JazzProvider({
  config,
  fallback,
  children,
  onJWTExpired,
  autoAttachDevTools,
  wasmSchema,
}: JazzProviderProps) {
  const shouldAutoAttach = process.env.NODE_ENV !== "production" && autoAttachDevTools !== false;
  return (
    <CoreJazzProvider
      config={config}
      fallback={fallback}
      createJazzClient={createJazzClient}
      onJWTExpired={onJWTExpired}
    >
      {shouldAutoAttach ? <DevToolsAutoAttach wasmSchema={wasmSchema} /> : null}
      {children}
    </CoreJazzProvider>
  );
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb(): Db {
  return useCoreDb<Db>();
}

export { useSession };

export type { JazzClientContextValue };
