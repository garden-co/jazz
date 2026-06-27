import type { Db, DbConfig } from "./db.js";
import type { Session } from "../runtime/context.js";
import { createReactNativeCoreUnsupportedError } from "./runtime-module.js";

export interface JazzClient {
  db: Db;
  session: Session | null;
  shutdown(): Promise<void>;
}

async function createJazzClientInternal(config: DbConfig): Promise<JazzClient> {
  void config;
  throw createReactNativeCoreUnsupportedError();
}

export function createJazzClient(config: DbConfig): Promise<JazzClient> {
  return createJazzClientInternal(config);
}
