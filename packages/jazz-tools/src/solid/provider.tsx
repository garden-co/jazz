import { createContext, useContext, type JSX, type Accessor, Show, createEffect } from "solid-js";
import type { Session } from "../runtime/context.js";
import {
  isPendingSolidJazzClientReady,
  type SolidJazzClient,
  type PendingSolidJazzClient,
} from "./create-solid-jazz-client.js";
import { Db } from "../runtime/db.js";
import { startInspectorOnce } from "../dev-tools/auto-attach.js";

type JazzClientContextValue = SolidJazzClient;

export const JazzClientContext = createContext<JazzClientContextValue | undefined>(undefined);

export type JazzProviderProps = {
  client: PendingSolidJazzClient;
  fallback?: JSX.Element;
  children: JSX.Element;
  autoAttachDevTools?: boolean;
};

export function JazzProvider(props: JazzProviderProps) {
  const clientReady = () =>
    isPendingSolidJazzClientReady(props.client) ? props.client : undefined;

  if (process.env.NODE_ENV !== "production" && props.autoAttachDevTools !== false) {
    createEffect(() => {
      const client = clientReady();
      if (client) startInspectorOnce(client.db as object);
    });
  }

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
