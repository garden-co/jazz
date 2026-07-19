import { createContext, useContext, type JSX, type Accessor, Show } from "solid-js";
import type { Session } from "../runtime/context.js";
import {
  isPendingSolidJazzClientReady,
  type SolidJazzClient,
  type PendingSolidJazzClient,
} from "./create-solid-jazz-client.js";
import { Db } from "../runtime/db.js";

type JazzClientContextValue = SolidJazzClient;

export const JazzClientContext = createContext<JazzClientContextValue | undefined>(undefined);

export type JazzProviderProps = {
  client: PendingSolidJazzClient;
  fallback?: JSX.Element;
  children: JSX.Element;
};

export function JazzProvider(props: JazzProviderProps) {
  const clientReady = () =>
    isPendingSolidJazzClientReady(props.client) ? props.client : undefined;

  return (
    <Show when={clientReady()} keyed fallback={props.fallback ?? null}>
      {(client) => (
        <JazzClientContext.Provider value={client}>{props.children}</JazzClientContext.Provider>
      )}
    </Show>
  );
}

export function useJazzClient(): JazzClientContextValue {
  const ctx = useContext(JazzClientContext);
  if (!ctx) throw new Error("useJazzClient must be used inside JazzProvider.");
  return ctx;
}

export function useDb<TDb = Db>(): Accessor<TDb> {
  const client = useJazzClient();
  return () => client.db as TDb;
}

export function useSession(): Accessor<Session | null> {
  const client = useJazzClient();
  return () => client.session;
}

export function useAuthState() {
  const client = useJazzClient();
  return () => client.authState;
}
