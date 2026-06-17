import type { DehydratedSnapshot } from "../backend/ssr.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import type { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { applyDelta } from "../reconcile-array.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import { applySnapshot } from "../ssr/apply-snapshot.js";
import { getJazzContext, type JazzContext } from "./context.svelte.js";

type MaybeGetter<T> = T | (() => T);

/**
 * Query options for a {@link QuerySubscription}, plus an optional SSR snapshot.
 * The whole bag may be passed as a getter to make the query options reactive; a
 * `snapshot` can ride along in that getter, so reactive options and a one-shot
 * snapshot can be combined, e.g. `() => ({ tier, snapshot })`.
 */
type QuerySubscriptionOptions = QueryOptions & {
  /**
   * A server-rendered snapshot for this query, co-located at the call site.
   * Seeds rows for synchronous first paint and queues its sync bundle for
   * flash-free hydration when the db attaches. Applied once at construction and
   * never reactive: a snapshot that changes afterwards is ignored, with a
   * development warning.
   */
  snapshot?: DehydratedSnapshot;
};

function resolve<T>(value: MaybeGetter<T>): T {
  return typeof value === "function" ? (value as () => T)() : value;
}

/**
 * Reactive query subscription. Instantiate in a component script block,
 * access results via `.current`.
 *
 * @param query - the database query, or a getter for a dynamic query
 *   (e.g. `() => filter ? app.todos.where({ title: { contains: filter } }) : undefined`).
 *   When a getter is passed, any reactive reads inside it are tracked, so the
 *   subscription re-runs when its dependencies change.
 * @param options - optional query execution options, or a getter for them. The
 *   bag may carry a one-shot `snapshot` in either form; the snapshot seeds once
 *   and is not reactive.
 *
 * ```svelte
 * <script lang="ts">
 *   const todos = new QuerySubscription(app.todos.where({ done: false }), { tier: "edge" });
 * </script>
 *
 * {#if todos.loading}
 *   <p>Loading...</p>
 * {:else if todos.error}
 *   <p>Error: {todos.error.message}</p>
 * {:else}
 *   {#each todos.current ?? [] as todo}
 *     <p>{todo.title}</p>
 *   {/each}
 * {/if}
 * ```
 */
export class QuerySubscription<T extends { id: string }> {
  current: T[] | undefined = $state();
  loading: boolean = $state(true);
  error: Error | null = $state(null);

  #snapshotApplied = false;
  #appliedSnapshot: DehydratedSnapshot | undefined;
  #snapshotChangeWarned = false;

  constructor(
    query: MaybeGetter<QueryBuilder<T> | undefined>,
    options?: MaybeGetter<QuerySubscriptionOptions | undefined>,
  ) {
    const ctx = getJazzContext();

    // Synchronous seed + first read: runs on the server (where $effect never
    // fires) and on the client's first init, so the seeded rows are in the SSR
    // HTML and the first paint — flash-free when live sync connects. The $effect
    // below re-reads (idempotent) and takes over for the live subscription.
    const initialQuery = resolve(query);
    if (initialQuery && ctx.manager) {
      const queryKeyOptions = this.#seed(ctx.manager, ctx, initialQuery, resolve(options));
      try {
        const entry = ctx.manager.getCacheEntry<T>(
          ctx.manager.makeQueryKey(initialQuery, queryKeyOptions),
        );
        if (entry.state.status === "fulfilled") {
          this.current = entry.state.data;
          this.loading = false;
        }
      } catch {
        // Any error surfaces through the $effect subscription on the client.
      }
    }

    $effect(() => {
      const resolvedQuery = resolve(query);
      if (!resolvedQuery) {
        this.current = undefined;
        this.loading = false;
        this.error = null;
        return;
      }

      const manager = ctx.manager;
      if (!manager) return;

      const queryKeyOptions = this.#seed(manager, ctx, resolvedQuery, resolve(options));

      this.loading = true;
      this.error = null;

      // Capture the unsubscribe in a local and return it directly, so the
      // effect's own teardown (on re-run and on root/component destroy) owns
      // the lifecycle. No shared mutable field to clobber, and no onDestroy —
      // which lets the class be used inside `$effect.root` and `.svelte.ts`.
      let unsubscribe: (() => void) | null = null;
      try {
        const key = manager.makeQueryKey(resolvedQuery, queryKeyOptions);
        const entry = manager.getCacheEntry<T>(key);

        // Apply initial state from cache
        if (entry.state.status === "fulfilled") {
          this.current = entry.state.data;
          this.loading = false;
        }

        unsubscribe = entry.subscribe({
          onfulfilled: (data: T[]) => {
            this.current = data;
            this.loading = false;
            this.error = null;
          },
          onDelta: (delta: SubscriptionDelta<T>) => {
            if (this.current) {
              applyDelta(this.current, delta);
            } else {
              this.current = delta.all;
            }
          },
          onError: (error: unknown) => {
            this.error = error instanceof Error ? error : new Error(String(error));
            this.current = undefined;
            this.loading = false;
          },
          onReset: () => {
            this.current = undefined;
            this.error = null;
            this.loading = true;
          },
        });
      } catch (e) {
        this.error = e instanceof Error ? e : new Error(String(e));
        this.loading = false;
      }

      return () => {
        unsubscribe?.();
      };
    });
  }

  // Split the snapshot out of the options (it must not affect the query key) and
  // apply it once — seeding the rows and queueing the bundle. Returns the options
  // to use for the query key (undefined when none remain).
  #seed(
    manager: SubscriptionsOrchestrator,
    ctx: JazzContext,
    query: QueryBuilder<T>,
    resolvedOptions: QuerySubscriptionOptions | undefined,
  ): QueryOptions | undefined {
    let snapshot: DehydratedSnapshot | undefined;
    let queryKeyOptions: QueryOptions | undefined = resolvedOptions;
    if (resolvedOptions && "snapshot" in resolvedOptions) {
      const { snapshot: snap, ...rest } = resolvedOptions;
      snapshot = snap;
      queryKeyOptions = Object.keys(rest).length > 0 ? rest : undefined;
    }

    if (snapshot) {
      if (!this.#snapshotApplied) {
        this.#snapshotApplied = true;
        this.#appliedSnapshot = snapshot;
        applySnapshot({
          manager,
          snapshot,
          // The client's own fingerprint comes from the query's schema: a snapshot
          // built against a different schema is skipped, not seeded.
          expected: {
            principalId: ctx.session?.user_id ?? null,
            schemaFingerprint: computeSchemaFingerprint(query._schema),
          },
        });
      } else if (snapshot !== this.#appliedSnapshot && !this.#snapshotChangeWarned) {
        // A snapshot seeds the store once at construction; a later, different one
        // (e.g. reactive state passed through the options getter) is ignored, so
        // flag it rather than letting it silently go stale.
        this.#snapshotChangeWarned = true;
        console.warn(
          "[jazz] QuerySubscription: the `snapshot` changed after the first render and was " +
            "ignored. An SSR snapshot seeds the store once at construction; let live sync deliver " +
            "later updates instead of swapping the snapshot.",
        );
      }
    }

    return queryKeyOptions;
  }
}
