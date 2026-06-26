import { useEffect, useMemo, type ReactNode } from "react";
import type { Session } from "../runtime/context.js";
import type { Db, DbConfig } from "../runtime/db.js";
import type { WasmSchema } from "../index.js";
import { markDevToolsAttached } from "../dev-tools/auto-attach.js";
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

function DevToolsAutoAttach({ wasmSchema }: { wasmSchema?: WasmSchema }) {
  const { db } = useCoreJazzClient() as JazzClientContextValue;
  useEffect(() => {
    if (!wasmSchema || !markDevToolsAttached(db as object)) return;
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
  // Enable devMode at client creation so the inspector's Live Query view can see
  // subscriptions the app creates on mount. devMode gates subscription tracing;
  // turning it on later (when attachDevTools runs) misses those first subscriptions.
  const effectiveConfig = useMemo(
    () => (shouldAutoAttach ? { ...config, devMode: true } : config),
    [config, shouldAutoAttach],
  );
  return (
    <CoreJazzProvider
      config={effectiveConfig}
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
