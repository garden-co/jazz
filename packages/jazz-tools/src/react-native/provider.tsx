import type { ReactNode } from "react";
import type { Db, DbConfig } from "./db.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
} from "../react-core/provider.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import type { Session } from "../runtime/context.js";

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  manager: CreatedJazzClient["manager"];
  session: Session | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
};

export function JazzProvider({ config, fallback, children }: JazzProviderProps) {
  return (
    <CoreJazzProvider config={config} fallback={fallback} createJazzClient={createJazzClient}>
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

export type { JazzClientContextValue };
export { useSession };
