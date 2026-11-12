<script lang="ts">
  import { getDb, getJazzSession, getSession } from 'jazz-tools/svelte';
  import { app } from '../schema.js';

  // #region db-access-svelte
  // Callable anywhere — component, store, or utility module
  const db = getDb();

  async function addTodo(title: string) {
    await db.insert(app.todos, { title, done: false });
  }
  // #endregion db-access-svelte

  // #region session-svelte
  // Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null
  const jazzSession = getJazzSession(); // $jazzSession.user
  // What your permissions see as `session` (its `user` is the account id).
  const session = getSession(); // { current: Session | null }
  // #endregion session-svelte

  void addTodo;
  void jazzSession;
  void session;
</script>
