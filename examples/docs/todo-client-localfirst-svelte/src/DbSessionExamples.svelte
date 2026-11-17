<script lang="ts">
  import type { Db } from 'jazz-tools';
  import { getDb, getJazzSession } from 'jazz-tools/svelte';
  import { app } from '../jazz.generated.js';

  // #region db-access-svelte
  // During component setup: getDb() reads Svelte context.
  const db = getDb();

  async function addTodo(title: string) {
    await db.insert(app.todos, { title, done: false });
  }

  // Stores and utility modules can't read context: pass them the db from here.
  function createTodoActions(database: Db) {
    return {
      complete: (id: string) => database.update(app.todos, id, { done: true }),
    };
  }
  const todoActions = createTodoActions(db);
  // #endregion db-access-svelte

  // #region session-svelte
  // Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null.
  // Your permissions see the same user as `session.user` (the accountId),
  // `session.identity` and `session.claims`.
  const jazzSession = getJazzSession(); // $jazzSession.user
  // #endregion session-svelte

  void addTodo;
  void todoActions;
  void jazzSession;
</script>
