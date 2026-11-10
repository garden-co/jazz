import { type ReactNode } from "react";
import type { PublicSession } from "../runtime/context.js";
import type { AccountDbConfig as DbConfig } from "../accounts/context.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type OpenJazzClient,
} from "../react-core/provider.js";
import { ConfiguredJazzAppProvider, type JazzAppViewProps } from "../react-core/app.js";
import { createJazzClient, type JazzClientConfig } from "../session/create-jazz-session.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import { refuseAutoAttachDevTools } from "../runtime/removed-provider-props.js";

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: CreatedJazzClient["db"];
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

// A provider given an explicit `config` opens the local-first client. It loads
// on first use, so apps on the (default) thin client never bundle it eagerly.
const createClient: OpenJazzClient = async (config) => {
  const { openJazzClient } = await import("../web/create-jazz-client.js");
  return (await openJazzClient(config)) as CreatedJazzClient;
};

type JazzProviderCommonProps = {
  fallback?: ReactNode;
  children: ReactNode;
};

export type JazzAppProviderProps = JazzClientConfig & JazzAppViewProps<CreatedJazzClient>;
export type JazzProviderProps =
  | (JazzProviderCommonProps & { config: DbConfig })
  | JazzAppProviderProps;

type ConfiguredJazzProviderProps = JazzProviderCommonProps & {
  config: DbConfig;
};

function ConfiguredJazzProvider({ config, fallback, children }: ConfiguredJazzProviderProps) {
  return (
    <CoreJazzProvider config={config} fallback={fallback} openJazzClient={createClient}>
      {children}
    </CoreJazzProvider>
  );
}

export function JazzProvider(props: JazzProviderProps) {
  refuseAutoAttachDevTools("JazzProvider", props);
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
  // Every other prop is client config, which reaches the browser worker:
  // a React element there fails to clone, far from its cause.
  if ("fallback" in props) {
    throw new Error(
      "<JazzProvider serverUrl=…> shows `loading` while it starts and `signedOut` while no one is signed in; it has no `fallback` prop (that is <JazzSessionProvider>'s and <JazzProvider config={…}>'s).",
    );
  }
  const { children, signedOut, loading, error, ...config } = props;
  return (
    <ConfiguredJazzAppProvider
      config={config}
      createJazzClient={createJazzClient}
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
      {children}
    </ConfiguredJazzAppProvider>
  );
}
