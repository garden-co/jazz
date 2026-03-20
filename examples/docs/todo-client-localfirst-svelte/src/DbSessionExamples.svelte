<script lang="ts">
  import { getDb, getSession } from 'jazz-tools/svelte';
  import { app } from '../schema/app.js';

  // #region db-access-svelte
  // Callable anywhere — component, store, or utility module
  const db = getDb();

  async function addTodo(title: string) {
    await db.insert(app.todos, { title, done: false });
  }
  // #endregion db-access-svelte

  // #region session-svelte
  const session = getSession(); // { user_id: string } | null
  // #endregion session-svelte

  void addTodo;
  void session;
</script>
