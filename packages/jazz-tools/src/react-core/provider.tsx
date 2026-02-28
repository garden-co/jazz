import { createContext, use, useContext, type ReactNode } from "react";
import type { Session } from "../runtime/context.js";
import type { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

type CoreJazzClient = {
  db: unknown;
  manager: SubscriptionsOrchestrator;
  session?: Session | null;
  shutdown: () => Promise<void>;
};

type JazzProviderClientProps = {
  client: CoreJazzClient | Promise<CoreJazzClient>;
  children: ReactNode;
};

export type JazzProviderProps = JazzProviderClientProps;

const JazzContext = createContext<CoreJazzClient | null>(null);

export function JazzProvider({ client, children }: JazzProviderProps) {
  const isPromiseLike = typeof client === "object" && client !== null && "then" in client;
  const resolvedClient = isPromiseLike ? use(client) : client;

  if (!resolvedClient) {
    throw new Error("JazzProvider requires a `client` prop.");
  }

  return <JazzContext.Provider value={resolvedClient}>{children}</JazzContext.Provider>;
}

export function useJazzClient(): CoreJazzClient {
  const ctx = useContext(JazzContext);
  if (!ctx) throw new Error("useDb must be used within <JazzProvider>");
  return ctx;
}

export function useDb<TDb = unknown>(): TDb {
  return useJazzClient().db as TDb;
}

export function useSession(): Session | null {
  return useJazzClient().session ?? null;
}
