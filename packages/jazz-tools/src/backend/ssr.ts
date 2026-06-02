import {
  computeSchemaFingerprint,
  resolveWasmSchema,
  type WasmSchemaInput,
} from "../drivers/schema-wire.js";
import type { Session } from "../runtime/context.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { computeQueryKey } from "../subscriptions-orchestrator.js";

export type SnapshotEntry = {
  key: string;
  result: unknown;
};

/**
 * Serialised, server-rendered query results. Passed across the
 * server→client boundary as a plain JSON value and consumed by
 * `JazzProvider` via its `snapshot` prop.
 */
export type DehydratedSnapshot = {
  appId: string;
  principalId: string | null;
  schemaFingerprint: string;
  entries: SnapshotEntry[];
};

export type SnapshotBuilderConfig = {
  /** Must match the client's `DbConfig.appId`. */
  appId: string;
  /**
   * Schema used to derive the envelope fingerprint. Accepts either the
   * raw `WasmSchema` or your merged `app` (whose `wasmSchema` field is
   * unwrapped automatically).
   */
  schema: WasmSchemaInput;
  /**
   * Optional. The snapshot's principal is normally derived from the `Db` you
   * prefetch with, so it can't drift from the data's scope. Pass this only to
   * set it explicitly (e.g. for a minimal `Db` that doesn't expose its session);
   * if it disagrees with the prefetched Db's principal, `dehydrate()` throws.
   */
  principalId?: string | null;
  /**
   * Milliseconds to wait for each prefetched query's first result before
   * rejecting, so a query that never delivers can't hang the server render.
   * Defaults to 30000; set to 0 to disable.
   */
  prefetchTimeoutMs?: number;
};

/**
 * Minimal `Db` subset the SSR builder needs to run a one-shot query.
 * The real `Db` from `JazzContext.forSession()` / `asBackend()` satisfies
 * this; the type avoids importing the full class to keep the surface
 * pluggable for tests.
 */
export type PrefetchableDb = {
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void;
  /**
   * The principal this `Db` reads as. When present, the snapshot's `principalId`
   * is derived from it so it can't drift from the data's scope. Optional so a
   * minimal `Db` (e.g. a test double) still satisfies the type.
   */
  getAuthState?(): { session: Session | null };
};

export type SnapshotBuilder = {
  prefetch<T extends { id: string }>(
    db: PrefetchableDb,
    query: QueryBuilder<T>,
    options?: QueryOptions,
    session?: Session,
  ): Promise<T[]>;
  dehydrate(): DehydratedSnapshot;
};

const DEFAULT_PREFETCH_TIMEOUT_MS = 30_000;

export function createSnapshotBuilder(config: SnapshotBuilderConfig): SnapshotBuilder {
  const entries = new Map<string, SnapshotEntry>();
  const schemaFingerprint = computeSchemaFingerprint(resolveWasmSchema(config.schema));
  const timeoutMs = config.prefetchTimeoutMs ?? DEFAULT_PREFETCH_TIMEOUT_MS;

  // The snapshot's principal is derived from the Db each prefetch runs against,
  // so it can't drift from the data's actual scope. Stays `undefined` until a Db
  // that exposes its session is prefetched (minimal Dbs fall back to config).
  let derivedPrincipalId: string | null | undefined;

  return {
    async prefetch<T extends { id: string }>(
      db: PrefetchableDb,
      query: QueryBuilder<T>,
      options?: QueryOptions,
      session?: Session,
    ): Promise<T[]> {
      const result = await prefetchOnce<T>(db, query, options, session, timeoutMs);
      const key = computeQueryKey(config.appId, query, options);
      entries.set(key, { key, result });

      const principal = readPrefetchPrincipal(db, session);
      if (principal !== undefined) {
        if (derivedPrincipalId !== undefined && derivedPrincipalId !== principal) {
          throw new Error(
            `[jazz] this snapshot builder prefetched as more than one principal (${JSON.stringify(
              derivedPrincipalId,
            )} and ${JSON.stringify(principal)}); a snapshot must be scoped to a single principal.`,
          );
        }
        derivedPrincipalId = principal;
      }

      return result;
    },
    dehydrate(): DehydratedSnapshot {
      return {
        appId: config.appId,
        principalId: resolvePrincipalId(derivedPrincipalId, config.principalId),
        schemaFingerprint,
        entries: Array.from(entries.values()),
      };
    },
  };
}

/**
 * The principal a prefetch ran as: the explicit `session` if one was passed,
 * else the Db's own session. `undefined` when the Db can't be introspected.
 */
function readPrefetchPrincipal(db: PrefetchableDb, session?: Session): string | null | undefined {
  if (session) {
    return session.user_id ?? null;
  }
  if (typeof db.getAuthState !== "function") {
    return undefined;
  }
  return db.getAuthState().session?.user_id ?? null;
}

function resolvePrincipalId(
  derived: string | null | undefined,
  configured: string | null | undefined,
): string | null {
  if (derived === undefined) {
    // No introspectable Db was prefetched; fall back to the configured value.
    return configured ?? null;
  }
  if (configured !== undefined && (configured ?? null) !== derived) {
    throw new Error(
      `[jazz] snapshot principalId ${JSON.stringify(
        configured,
      )} disagrees with the principal the prefetch ran as (${JSON.stringify(
        derived,
      )}). Omit principalId — it is derived from the Db you prefetch with.`,
    );
  }
  return derived;
}

function prefetchOnce<T extends { id: string }>(
  db: PrefetchableDb,
  query: QueryBuilder<T>,
  options?: QueryOptions,
  session?: Session,
  timeoutMs = DEFAULT_PREFETCH_TIMEOUT_MS,
): Promise<T[]> {
  return new Promise<T[]>((resolve, reject) => {
    let unsubscribe: (() => void) | null = null;
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const settle = () => {
      settled = true;
      if (timer) clearTimeout(timer);
    };

    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        if (settled) return;
        settle();
        unsubscribe?.();
        reject(
          new Error(
            `[jazz] prefetch timed out after ${timeoutMs}ms waiting for the query's first result.`,
          ),
        );
      }, timeoutMs);
    }

    try {
      unsubscribe = db.subscribeAll<T>(
        query,
        (delta) => {
          if (settled) return;
          settle();
          resolve(delta.all);
          queueMicrotask(() => {
            unsubscribe?.();
          });
        },
        options,
        session,
      );
    } catch (error) {
      settle();
      reject(error);
    }
  });
}
