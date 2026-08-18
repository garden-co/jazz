import { applyDelta } from "../shared/index.js";
import { limitQueryToOne } from "../runtime/db.js";
import type { QueryBuilder, QueryOptions, SubscriptionDelta } from "../shared/index.js";
import { getJazzContext } from "./context.svelte.js";

type MaybeGetter<T> = T | (() => T);

type QuerySubscriptionOptions<One extends boolean> = One extends true
  ? QueryOptions & { one: true }
  : MaybeGetter<(QueryOptions & { one?: false }) | undefined>;

type QuerySubscriptionResult<T, One extends boolean> = One extends true
  ? T | null | undefined
  : T[] | undefined;

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
export class QuerySubscription<T extends { id: string }, One extends boolean = false> {
  current: QuerySubscriptionResult<T, One> = $state();
  loading: boolean = $state(true);
  error: Error | null = $state(null);

  constructor(
    query: MaybeGetter<QueryBuilder<T> | undefined>,
    options?: QuerySubscriptionOptions<One>,
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

      const store = ctx.subscriptionStore;
      if (!store) return;

      const resolvedOptions = resolve(
        options as MaybeGetter<(QueryOptions & { one?: boolean }) | undefined>,
      );
      const one = resolvedOptions?.one === true;
      const queryOptions = resolvedOptions && (({ one: _, ...rest }) => rest)(resolvedOptions);
      const subscriptionQuery = one ? limitQueryToOne(resolvedQuery) : resolvedQuery;

      this.loading = true;
      this.error = null;

      // Capture the unsubscribe in a local and return it directly, so the
      // effect's own teardown (on re-run and on root/component destroy) owns
      // the lifecycle. No shared mutable field to clobber, and no onDestroy —
      // which lets the class be used inside `$effect.root` and `.svelte.ts`.
      let unsubscribe: (() => void) | null = null;
      try {
        const key = store.makeQueryKey(subscriptionQuery, queryOptions);
        const entry = store.getCacheEntry<T>(key);

        // Apply initial state from cache
        if (entry.state.status === "fulfilled") {
          this.current = (
            one ? (entry.state.data[0] ?? null) : entry.state.data
          ) as QuerySubscriptionResult<T, One>;
          this.loading = false;
        }

        unsubscribe = entry.subscribe({
          onfulfilled: (data: T[]) => {
            this.current = (one ? (data[0] ?? null) : data) as QuerySubscriptionResult<T, One>;
            this.loading = false;
            this.error = null;
          },
          onDelta: (delta: SubscriptionDelta<T>) => {
            if (one) {
              this.current = (delta.all[0] ?? null) as QuerySubscriptionResult<T, One>;
            } else if (this.current) {
              applyDelta(this.current as T[], delta);
            } else if (delta.reset) {
              this.current = delta.all as QuerySubscriptionResult<T, One>;
            } else {
              this.current = [] as unknown as QuerySubscriptionResult<T, One>;
              applyDelta(this.current as T[], delta);
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
