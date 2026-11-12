import type { PublicSession } from "../runtime/context.js";
import {
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
} from "../react-core/provider.js";
import {
  JazzSessionProvider,
  type JazzSessionSourceProps,
  type JazzSessionViewProps,
} from "../react-core/session.js";
import { openBrowserSessionCore, type JazzClientConfig } from "../session/create-jazz-session.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import {
  refuseAutoAttachDevTools,
  type NoAutoAttachDevTools,
} from "../runtime/removed-provider-props.js";
import { refuseRemovedProviderProps } from "./provider-props.js";

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: CreatedJazzClient["db"];
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type JazzProviderProps = JazzSessionSourceProps<JazzClientConfig, CreatedJazzClient> &
  JazzSessionViewProps<CreatedJazzClient> &
  NoAutoAttachDevTools;

/**
 * Jazz for a React app: owns a session (`config`) or shows yours (`session`),
 * renders `children` with its client once someone is signed in, and
 * `signedOut`, `loading` or `error` otherwise. Read the session anywhere
 * below with `useJazzSession()`.
 */
export function JazzProvider(props: JazzProviderProps) {
  refuseAutoAttachDevTools("JazzProvider", props);
  refuseRemovedProviderProps(props);
  const { loading, error, autoAttachDevTools: _removed, ...rest } = props;
  return (
    <JazzSessionProvider
      {...rest}
      openCore={openBrowserSessionCore}
      loading={loading === undefined ? <p role="status">Loading…</p> : loading}
      error={
        error === undefined
          ? (state) => (
              <section role="alert">
                <p>Jazz couldn’t start. Please try again.</p>
                <button type="button" onClick={() => void state.retry().catch(() => {})}>
                  Try again
                </button>
              </section>
            )
          : error
      }
    />
  );
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
