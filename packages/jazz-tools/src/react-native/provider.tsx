import type { ReactNode } from "react";
import type { Session } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type CreateJazzClient,
} from "../react-core/provider.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import type { DbConfig } from "./create-db.js";

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  session: Session | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
};

export function JazzProvider({ config, fallback, children, onJWTExpired }: JazzProviderProps) {
  const createClient: CreateJazzClient = (nextConfig) =>
    createJazzClient(nextConfig as DbConfig) as Promise<CreatedJazzClient>;

  return (
    <CoreJazzProvider
      config={config}
      fallback={fallback}
      createJazzClient={createClient}
      onJWTExpired={onJWTExpired}
    >
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
