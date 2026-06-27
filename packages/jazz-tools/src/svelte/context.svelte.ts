import { getContext, setContext } from "svelte";
import type { Db } from "../runtime/db.js";
import type { Session } from "../runtime/context.js";
import type { SubscriptionStore } from "../subscription-store-internal.js";

const JAZZ_CTX_KEY = Symbol("jazz");

export interface JazzContext {
  db: Db | null;
  session: Session | null;
  /** @internal Used by framework bindings; not part of the app-facing client API. */
  subscriptionStore: SubscriptionStore | null;
}

/**
 * Initialize the Jazz context for descendant Svelte components.
 * Called internally by {@link JazzSvelteProvider}.
 */
export function initJazzContext(): JazzContext {
  const ctx: JazzContext = $state({ db: null, session: null, subscriptionStore: null });
  setContext(JAZZ_CTX_KEY, ctx);
  return ctx;
}

/**
 * Get the current Jazz context, including the backing {@link Db} and session snapshot.
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
 * Subscribe to the current Jazz {@link Session}.
 * The returned handle's `.current` property always reflects the latest session,
 * updating automatically as the user logs in or out.
 */
export function getSession(): { readonly current: Session | null } {
  const ctx = getJazzContext();
  return {
    get current() {
      return ctx.session;
    },
  };
}
