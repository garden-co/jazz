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

/**
 * Initialize the Jazz context for descendant Svelte components.
 * Called internally by {@link JazzSvelteProvider}.
 */
export function initJazzContext(): JazzContext {
  const ctx: JazzContext = $state({ db: null, session: null, manager: null });
  setContext(JAZZ_CTX_KEY, ctx);
  return ctx;
}

/**
 * Get the current Jazz context, including the backing {@link Db}, session snapshot,
 * and subscription manager.
 */
export function getJazzContext(): JazzContext {
  const ctx = getContext<JazzContext | undefined>(JAZZ_CTX_KEY);
  if (!ctx) {
    throw new Error("getDb/getSession must be used within <JazzSvelteProvider>");
  }
  return ctx;
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function getDb(): Db {
  const ctx = getJazzContext();
  if (!ctx.db) {
    throw new Error("Jazz database is not yet initialised");
  }
  return ctx.db;
}

/**
 * Get the current Jazz {@link Session}, including the user's id, claims and auth mode.
 */
export function getSession(): Session | null {
  return getJazzContext().session;
}
