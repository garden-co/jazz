import type { QueryBuilder, QueryOptions } from "../shared/index.js";
import { limitQueryToOne } from "../shared/index.js";
import { QuerySubscription } from "./use-all.svelte.js";

type MaybeGetter<T> = T | (() => T);

function resolve<T>(value: MaybeGetter<T>): T {
  return typeof value === "function" ? (value as () => T)() : value;
}

/**
 * Reactive single-row subscription. Like {@link QuerySubscription} but narrowed
 * to one row: the query is executed with `limit 1` (the same wrapper `Db.one`
 * uses, so the two stay in lockstep), and the subscription tracks just that row.
 *
 * Instantiate in a component script block, access the result via `.current`,
 * which is the matching row, `null` once the query resolves with no match, or
 * `undefined` while loading.
 *
 * @param query - the database query, or a getter for a dynamic query
 *   (e.g. `() => id ? app.todos.where({ id }) : undefined`).
 *   When a getter is passed, any reactive reads inside it are tracked, so the
 *   subscription re-runs when its dependencies change.
 * @param options - optional query execution options, or a getter for them
 *
 * ```svelte
 * <script lang="ts">
 *   const todo = new SingleRowSubscription(app.todos.where({ id }), { tier: "edge" });
 * </script>
 *
 * {#if todo.loading}
 *   <p>Loading...</p>
 * {:else if todo.error}
 *   <p>Error: {todo.error.message}</p>
 * {:else if todo.current}
 *   <p>{todo.current.title}</p>
 * {:else}
 *   <p>Not found</p>
 * {/if}
 * ```
 */
export class SingleRowSubscription<T extends { id: string }> {
  #subscription: QuerySubscription<T>;

  constructor(
    query: MaybeGetter<QueryBuilder<T> | undefined>,
    options?: MaybeGetter<QueryOptions | undefined>,
  ) {
    this.#subscription = new QuerySubscription<T>(() => {
      const resolvedQuery = resolve(query);
      return resolvedQuery ? limitQueryToOne(resolvedQuery) : undefined;
    }, options);
  }

  get current(): T | null | undefined {
    const rows = this.#subscription.current;
    return rows === undefined ? undefined : (rows[0] ?? null);
  }

  get loading(): boolean {
    return this.#subscription.loading;
  }

  get error(): Error | null {
    return this.#subscription.error;
  }
}
