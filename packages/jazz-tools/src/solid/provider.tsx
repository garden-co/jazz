import { SessionContext } from "./session-context.js";
import { createContext, useContext, type JSX, type Accessor, Show } from "solid-js";
import {
  isPendingSolidJazzClientReady,
  type SolidJazzClient,
  type PendingSolidJazzClient,
} from "./create-solid-jazz-client.js";
import type { Db } from "../runtime/db.js";
import {
  refuseAutoAttachDevTools,
  type NoAutoAttachDevTools,
} from "../runtime/removed-provider-props.js";

type JazzClientContextValue = SolidJazzClient;

export const JazzClientContext = createContext<JazzClientContextValue | undefined>(undefined);

export type JazzClientProviderProps = NoAutoAttachDevTools & {
  client: PendingSolidJazzClient;
  fallback?: JSX.Element;
  children: JSX.Element;
};

export function JazzClientProvider(props: JazzClientProviderProps) {
  refuseAutoAttachDevTools("JazzClientProvider", props);
  const clientReady = () =>
    isPendingSolidJazzClientReady(props.client) ? props.client : undefined;

  return (
    <Show when={clientReady()} keyed fallback={props.fallback ?? null}>
      {(client) => (
        // Its children read this client, not an enclosing <JazzProvider>'s session.
        <SessionContext.Provider value={undefined}>
          <JazzClientContext.Provider value={client}>{props.children}</JazzClientContext.Provider>
        </SessionContext.Provider>
      )}
    </Show>
  );
}

export function useJazzClient(): JazzClientContextValue {
  const ctx = useContext(JazzClientContext);
  if (!ctx) {
    throw new Error("useJazzClient must be used inside JazzProvider or JazzClientProvider.");
  }
  return ctx;
}

export function useDb<TDb = Db>(): Accessor<TDb> {
  const client = useJazzClient();
  return () => client.db as TDb;
}

export function useAuthState() {
  const client = useJazzClient();
  return () => client.authState;
}

export { JazzProvider, type JazzProviderProps } from "./session.js";
