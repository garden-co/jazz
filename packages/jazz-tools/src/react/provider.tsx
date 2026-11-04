import { type ReactNode } from "react";
import type { PublicSession } from "../runtime/context.js";
import type { AccountDbConfig as DbConfig } from "../accounts/context.js";
import { jazzDevPluginActive } from "../dev-tools/auto-attach.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type CreateJazzClient,
} from "../react-core/provider.js";
import { ConfiguredJazzAppProvider, type JazzAppViewProps } from "../react-core/app.js";
import { createJazzSession, type JazzSessionConfig } from "../session/create-jazz-session.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import { DevToolsAutoAttach } from "./devtools-auto-attach.js";

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: CreatedJazzClient["db"];
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

// A provider given an explicit `config` opens the local-first client. It loads
// on first use, so apps on the (default) thin client never bundle it eagerly.
const createClient: CreateJazzClient = async (config) => {
  const { createJazzClient } = await import("../web/create-jazz-client.js");
  return (await createJazzClient(config)) as CreatedJazzClient;
};

type JazzProviderCommonProps = {
  fallback?: ReactNode;
  children: ReactNode;
  /** Dev-only: auto-open the inspector overlay. Default true. */
  autoAttachDevTools?: boolean;
};

export type JazzAppProviderProps = JazzSessionConfig &
  JazzAppViewProps<CreatedJazzClient> & {
    autoAttachDevTools?: boolean;
  };
export type JazzProviderProps =
  | (JazzProviderCommonProps & { config: DbConfig })
  | JazzAppProviderProps;

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
  if ("config" in props) return <ConfiguredJazzProvider {...props} />;
  return <ApplicationJazzProvider {...props} />;
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

function ApplicationJazzProvider(props: JazzAppProviderProps) {
  const { children, signedOut, loading, error, autoAttachDevTools, ...config } = props;
  const shouldAutoAttach = process.env.NODE_ENV !== "production" && autoAttachDevTools !== false;
  return (
    <ConfiguredJazzAppProvider
      config={config}
      createJazzSession={createJazzSession}
      signedOut={signedOut}
      loading={loading === undefined ? <p role="status">Loading…</p> : loading}
      error={
        error === undefined
          ? (state) => (
              <section role="alert">
                <p>We couldn’t connect to your account. Please try again.</p>
                <button type="button" onClick={() => void state.retry()}>
                  Try again
                </button>
              </section>
            )
          : error
      }
    >
      {shouldAutoAttach ? <DevToolsAutoAttach /> : null}
      {children}
    </ConfiguredJazzAppProvider>
  );
}
