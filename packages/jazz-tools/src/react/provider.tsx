import { useEffect, type ReactNode } from "react";
import type { PublicSession } from "../runtime/context.js";
import type { AccountDbConfig as DbConfig } from "../accounts/context.js";
import { jazzDevPluginActive, startInspectorOnce } from "../dev-tools/auto-attach.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type CreateJazzClient,
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
  db: CreatedJazzClient["db"];
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

const createClient: CreateJazzClient = (config) =>
  createJazzClient(config) as Promise<CreatedJazzClient>;

// Dev-only: mount the inspector overlay + publish the host handle for this db.
// Only rendered when shouldAutoAttach is true, so the lazy overlay chunk is
// dropped from production bundles.
function DevToolsAutoAttach() {
  const { db } = useCoreJazzClient() as JazzClientContextValue;
  useEffect(() => {
    startInspectorOnce(db);
  }, [db]);
  return null;
}

type JazzProviderCommonProps = {
  fallback?: ReactNode;
  children: ReactNode;
  /** Dev-only: auto-open the inspector overlay. Default true. */
  autoAttachDevTools?: boolean;
};

export type JazzProviderProps = JazzProviderCommonProps & { config: DbConfig };

type ConfiguredJazzProviderProps = JazzProviderCommonProps & {
  config: DbConfig;
};

function ConfiguredJazzProvider({
  config,
  fallback,
  children,
  autoAttachDevTools,
}: ConfiguredJazzProviderProps) {
  const shouldAutoAttach = process.env.NODE_ENV !== "production" && autoAttachDevTools !== false;
  // Subscription traces only register while devMode is on at subscribe time,
  // so it must be on from Db construction for the overlay's Subscriptions tab
  // to see the app's startup queries — the host bridge's later setDevMode(true)
  // only covers subscriptions opened after the overlay attached. Default it on
  // exactly when the overlay will mount; an explicit config value always wins.
  const effectiveConfig =
    shouldAutoAttach && config.devMode === undefined && jazzDevPluginActive()
      ? { ...config, devMode: true }
      : config;

  return (
    <CoreJazzProvider config={effectiveConfig} fallback={fallback} createJazzClient={createClient}>
      {shouldAutoAttach ? <DevToolsAutoAttach /> : null}
      {children}
    </CoreJazzProvider>
  );
}

export function JazzProvider(props: JazzProviderProps) {
  return <ConfiguredJazzProvider {...props} />;
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb(): CreatedJazzClient["db"] {
  return useCoreDb<CreatedJazzClient["db"]>();
}

export { useSession };

export type { JazzClientContextValue };
