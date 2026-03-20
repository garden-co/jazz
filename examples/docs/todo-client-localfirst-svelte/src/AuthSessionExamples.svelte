<script lang="ts">
  import { getDb, getSession, QuerySubscription } from 'jazz-tools/svelte';
  import { app } from '../schema/session-app.js';

  const db = getDb();

  // #region auth-session-svelte-hook
  const session = getSession();
  // #endregion auth-session-svelte-hook

  // #region auth-session-svelte-user-id
  const sessionUserId = $derived(session?.user_id ?? null);
  // #endregion auth-session-svelte-user-id

  // #region auth-session-svelte-query
  const ownedTodos = new QuerySubscription(
    app.todos.where({ owner_id: sessionUserId ?? '__no-session__' }),
  );
  // #endregion auth-session-svelte-query

  // #region auth-session-svelte-insert
  function addOwnedTodo(title: string) {
    if (!sessionUserId) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: sessionUserId,
    });
  }
  // #endregion auth-session-svelte-insert

  void ownedTodos;
  void addOwnedTodo;
</script>
