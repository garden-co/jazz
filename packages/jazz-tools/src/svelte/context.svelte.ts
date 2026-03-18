import { getContext, setContext } from "svelte";
import type { Db } from "../runtime/db.js";
import type { Session } from "../runtime/context.js";
import type { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

const JAZZ_CTX_KEY = Symbol("jazz");

export interface JazzContext {
  db: Db | null;
  session: Session | null;
  manager: SubscriptionsOrchestrator | null;
}

export function initJazzContext(): JazzContext {
  const ctx: JazzContext = $state({ db: null, session: null, manager: null });
  setContext(JAZZ_CTX_KEY, ctx);
  return ctx;
}

export function getJazzContext(): JazzContext {
  const ctx = getContext<JazzContext | undefined>(JAZZ_CTX_KEY);
  if (!ctx) {
    throw new Error("getDb/getSession must be used within <JazzSvelteProvider>");
  }
  return ctx;
}

export function getDb(): Db {
  const ctx = getJazzContext();
  if (!ctx.db) {
    throw new Error("Jazz database is not yet initialised");
  }
  return ctx.db;
}

export function getSession(): Session | null {
  return getJazzContext().session;
}
