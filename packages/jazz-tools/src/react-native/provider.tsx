import type { ReactNode } from "react";
import type { Db } from "./db.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
} from "../react-core/provider.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";

interface JazzClientContextValue {
  db: Db;
  manager: CreatedJazzClient["manager"];
  shutdown: CreatedJazzClient["shutdown"];
}

type JazzProviderClientProps = {
  client: CreatedJazzClient | Promise<CreatedJazzClient>;
  children: ReactNode;
};

export type JazzProviderProps = JazzProviderClientProps;

export function JazzProvider({ client, children }: JazzProviderProps) {
  return <CoreJazzProvider client={client}>{children}</CoreJazzProvider>;
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

export function useDb(): Db {
  return useCoreDb<Db>();
}

export type { JazzClientContextValue };
