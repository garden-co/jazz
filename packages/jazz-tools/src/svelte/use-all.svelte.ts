import { onDestroy } from "svelte";
import type { QueryBuilder } from "../runtime/db.js";
import type { PersistenceTier } from "../runtime/client.js";
import { getJazzContext } from "./context.svelte.js";

/**
 * Reactive query subscription. Instantiate in a component script block,
 * access results via `.current`.
 *
 * ```svelte
 * <script lang="ts">
 *   const todos = new QuerySubscription(app.todos.where({ done: false }));
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

  #unsubscribe: (() => void) | null = null;

  constructor(query: QueryBuilder<T>, tier?: PersistenceTier) {
    const ctx = getJazzContext();
    this.current = tier ? undefined : [];

    $effect(() => {
      const db = ctx.db;
      if (!db) return;

      this.loading = true;
      this.error = null;

      try {
        this.#unsubscribe = db.subscribeAll(
          query,
          (delta) => {
            this.current = delta.all;
            this.loading = false;
          },
          tier,
        );
      } catch (e) {
        this.error = e instanceof Error ? e : new Error(String(e));
        this.loading = false;
      }

      return () => {
        this.#cleanup();
      };
    });

    onDestroy(() => {
      this.#cleanup();
    });
  }

  #cleanup() {
    if (this.#unsubscribe) {
      this.#unsubscribe();
      this.#unsubscribe = null;
    }
  }
}
