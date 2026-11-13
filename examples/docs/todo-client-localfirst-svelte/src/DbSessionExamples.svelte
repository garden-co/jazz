<script lang="ts">
  import type { Db } from 'jazz-tools';
  import { getDb, getJazzSession, getSession } from 'jazz-tools/svelte';
  import { app } from '../schema.js';

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
  // Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null
  const jazzSession = getJazzSession(); // $jazzSession.user
  // What your permissions see as `session` (its `user` is the account id).
  const session = getSession(); // { current: Session | null }
  // #endregion session-svelte

  void addTodo;
  void todoActions;
  void jazzSession;
  void session;
</script>
