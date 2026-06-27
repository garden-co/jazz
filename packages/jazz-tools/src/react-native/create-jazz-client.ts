import type { Db, DbConfig } from "./db.js";
import type { Session } from "../runtime/context.js";
import type { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { createReactNativeDirectCoreAlphaUnsupportedError } from "./runtime-module.js";

export interface JazzClient {
  db: Db;
  session: Session | null;
  manager: SubscriptionsOrchestrator;
  shutdown(): Promise<void>;
}

async function createJazzClientInternal(config: DbConfig): Promise<JazzClient> {
  void config;
  throw createReactNativeDirectCoreAlphaUnsupportedError();
}

export function createJazzClient(config: DbConfig): Promise<JazzClient> {
  return createJazzClientInternal(config);
}
