import type { DehydratedSnapshot } from "../backend/ssr.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { applyDelta } from "../reconcile-array.js";
import { applySnapshot } from "../ssr/apply-snapshot.js";
import { getJazzContext } from "./context.svelte.js";

type MaybeGetter<T> = T | (() => T);

/** Query options for a {@link QuerySubscription}, plus an optional SSR snapshot. */
type QuerySubscriptionOptions = QueryOptions & {
  /**
   * A server-rendered snapshot for this query, co-located at the call site.
   * Seeds rows for synchronous first paint and queues its sync bundle for
   * flash-free hydration when the db attaches.
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
 * @param options - optional query execution options, or a getter for them
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

  constructor(
    query: MaybeGetter<QueryBuilder<T> | undefined>,
    options?: MaybeGetter<QuerySubscriptionOptions | undefined>,
  ) {
    const ctx = getJazzContext();

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

      const resolvedOptions = resolve(options);
      let snapshot: DehydratedSnapshot | undefined;
      // Strip the snapshot from the options used for the query key (it must not
      // affect the key), preserving `undefined` when no query options remain.
      let queryKeyOptions: QueryOptions | undefined = resolvedOptions;
      if (resolvedOptions && "snapshot" in resolvedOptions) {
        const { snapshot: snap, ...rest } = resolvedOptions;
        snapshot = snap;
        queryKeyOptions = Object.keys(rest).length > 0 ? rest : undefined;
      }

      // Apply the co-located snapshot once, before the first makeQueryKey
      // lookup, so the first render reads seeded rows. The orchestrator decides
      // what to do with it (seed + queue pre-attach; ignore post-attach).
      if (snapshot && !this.#snapshotApplied) {
        this.#snapshotApplied = true;
        applySnapshot({
          manager,
          snapshot,
          expected: { principalId: ctx.session?.user_id ?? null },
        });
      }

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
}
