import { createContext, use, useContext, type ReactNode } from "react";
import type { Session } from "../runtime/context.js";
import type { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

type CoreJazzClient = {
  db: unknown;
  manager: SubscriptionsOrchestrator;
  session?: Session | null;
  shutdown: () => Promise<void>;
};

export interface JazzClientContextValue {
  db: unknown;
  manager: SubscriptionsOrchestrator;
  session: Session | null;
  shutdown: () => Promise<void>;
}

type JazzProviderClientProps = {
  client: CoreJazzClient | Promise<CoreJazzClient>;
  children: ReactNode;
};

export type JazzProviderProps = JazzProviderClientProps;

const JazzContext = createContext<JazzClientContextValue | null>(null);

function getClientValue(client: CoreJazzClient): JazzClientContextValue {
  return {
    db: client.db,
    manager: client.manager,
    session: client.session ?? null,
    shutdown: client.shutdown,
  };
}

export function JazzProvider({ client, children }: JazzProviderProps) {
  const resolvedClient = "then" in client ? use(client) : client;

  return (
    <JazzContext.Provider value={getClientValue(resolvedClient)}>{children}</JazzContext.Provider>
  );
}

export function useJazzClient(): JazzClientContextValue {
  const ctx = useContext(JazzContext);
  if (!ctx) throw new Error("useDb must be used within <JazzProvider>");
  return ctx;
}

export function useDb<TDb = unknown>(): TDb {
  return useJazzClient().db as TDb;
}

export function useSession(): Session | null {
  return useJazzClient().session;
}
