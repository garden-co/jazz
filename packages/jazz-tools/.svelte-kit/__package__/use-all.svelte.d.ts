import type { QueryBuilder } from '../runtime/db.js';
import type { PersistenceTier } from '../runtime/client.js';
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
export declare class QuerySubscription<T extends {
    id: string;
}> {
    #private;
    current: T[] | undefined;
    loading: boolean;
    error: Error | null;
    constructor(query: QueryBuilder<T>, tier?: PersistenceTier);
}
//# sourceMappingURL=use-all.svelte.d.ts.map