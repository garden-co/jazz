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
  /** Identifier for the authenticated principal whose queries are being prefetched, or `null` for unauthenticated renders. */
  principalId?: string | null;
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

export function createSnapshotBuilder(config: SnapshotBuilderConfig): SnapshotBuilder {
  const entries = new Map<string, SnapshotEntry>();
  const schemaFingerprint = computeSchemaFingerprint(resolveWasmSchema(config.schema));

  return {
    async prefetch<T extends { id: string }>(
      db: PrefetchableDb,
      query: QueryBuilder<T>,
      options?: QueryOptions,
      session?: Session,
    ): Promise<T[]> {
      const result = await prefetchOnce<T>(db, query, options, session);
      const key = computeQueryKey(config.appId, query, options);
      entries.set(key, { key, result });
      return result;
    },
    dehydrate(): DehydratedSnapshot {
      return {
        appId: config.appId,
        principalId: config.principalId ?? null,
        schemaFingerprint,
        entries: Array.from(entries.values()),
      };
    },
  };
}

function prefetchOnce<T extends { id: string }>(
  db: PrefetchableDb,
  query: QueryBuilder<T>,
  options?: QueryOptions,
  session?: Session,
): Promise<T[]> {
  return new Promise<T[]>((resolve, reject) => {
    let unsubscribe: (() => void) | null = null;
    let settled = false;

    try {
      unsubscribe = db.subscribeAll<T>(
        query,
        (delta) => {
          if (settled) return;
          settled = true;
          resolve(delta.all);
          queueMicrotask(() => {
            unsubscribe?.();
          });
        },
        options,
        session,
      );
    } catch (error) {
      settled = true;
      reject(error);
    }
  });
}
