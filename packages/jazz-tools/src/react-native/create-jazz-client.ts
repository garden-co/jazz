import type { Db, DbConfig } from "./db.js";
import type { Session } from "../runtime/context.js";
import { createReactNativeDirectCoreUnsupportedError } from "./runtime-module.js";

export interface JazzClient {
  db: Db;
  session: Session | null;
  shutdown(): Promise<void>;
}

async function createJazzClientInternal(config: DbConfig): Promise<JazzClient> {
  void config;
  throw createReactNativeDirectCoreUnsupportedError();
}

export function createJazzClient(config: DbConfig): Promise<JazzClient> {
  return createJazzClientInternal(config);
}
