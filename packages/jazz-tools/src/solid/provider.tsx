import { createContext, useContext, type JSX, type Accessor, Show, createEffect } from "solid-js";
import type { PublicSession } from "../runtime/context.js";
import {
  createSolidJazzClient,
  isPendingSolidJazzClientReady,
  type SolidJazzClient,
  type PendingSolidJazzClient,
} from "./create-solid-jazz-client.js";
import type { Db, DbConfig } from "../runtime/db.js";
import { startInspectorOnce } from "../dev-tools/auto-attach.js";

type JazzClientContextValue = SolidJazzClient;

export const JazzClientContext = createContext<JazzClientContextValue | undefined>(undefined);

export type JazzClientProviderProps = {
  client: PendingSolidJazzClient;
  fallback?: JSX.Element;
  children: JSX.Element;
  autoAttachDevTools?: boolean;
};

export function JazzClientProvider(props: JazzClientProviderProps) {
  const clientReady = () =>
    isPendingSolidJazzClientReady(props.client) ? props.client : undefined;

  if (process.env.NODE_ENV !== "production" && props.autoAttachDevTools !== false) {
    createEffect(() => {
      const client = clientReady();
      if (client) startInspectorOnce(client.db);
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

export type JazzProviderProps = Omit<JazzClientProviderProps, "client"> & {
  config: DbConfig;
};

export function JazzProvider(props: JazzProviderProps) {
  const client = createSolidJazzClient(() => props.config);

  return (
    <JazzClientProvider
      client={client}
      fallback={props.fallback}
      autoAttachDevTools={props.autoAttachDevTools}
    >
      {props.children}
    </JazzClientProvider>
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

export function useSession(): Accessor<PublicSession | null> {
  const client = useJazzClient();
  return () => client.session;
}

export function useAuthState() {
  const client = useJazzClient();
  return () => client.authState;
}
