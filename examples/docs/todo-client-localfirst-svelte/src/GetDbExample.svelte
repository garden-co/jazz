<!-- #region writing-get-db-svelte -->
<script lang="ts">
  import { getDb } from 'jazz-tools/svelte';

  const db = getDb();

  // Query subscriptions carry the same handle, so a component that reads and writes needs one:
  // const todos = new QuerySubscription(app.todos);
  // todos.db.insert(app.todos, { title: 'Buy milk', done: false });
</script>
<!-- #endregion writing-get-db-svelte -->
