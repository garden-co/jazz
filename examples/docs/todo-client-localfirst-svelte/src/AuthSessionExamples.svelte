<script lang="ts">
	import { getDb, getSession, QuerySubscription } from 'jazz-tools/svelte';
	import { app } from '../session-app.js';

  const db = getDb();

  // #region auth-session-svelte-hook
  const session = getSession();
  // #endregion auth-session-svelte-hook

  // #region auth-session-svelte-user
  const sessionUser = $derived(session.current?.user ?? null);
  // #endregion auth-session-svelte-user

  // #region auth-session-svelte-query
  const ownedTodos = new QuerySubscription(
    () => (sessionUser ? app.todos.where({ owner_id: sessionUser }) : undefined),
  );
  // #endregion auth-session-svelte-query

  // #region auth-session-svelte-insert
  function addOwnedTodo(title: string) {
    if (!sessionUser) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: sessionUser,
    });
  }
  // #endregion auth-session-svelte-insert

  void ownedTodos;
  void addOwnedTodo;
</script>
