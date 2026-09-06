import type { ReactNode } from "react";
import type { PublicSession } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type CreateJazzClient,
} from "../react-core/provider.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import type { JazzClientConfig as DbConfig } from "./create-jazz-client.js";

const createClient: CreateJazzClient = (config) =>
  createJazzClient(config as DbConfig) as Promise<CreatedJazzClient>;

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
};

export function JazzProvider({ config, fallback, children }: JazzProviderProps) {
  return (
    <CoreJazzProvider config={config} fallback={fallback} createJazzClient={createClient}>
      {children}
    </CoreJazzProvider>
  );
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

export function useDb(): Db {
  return useCoreDb<Db>();
}

export { useSession };
export type { JazzClientContextValue };
