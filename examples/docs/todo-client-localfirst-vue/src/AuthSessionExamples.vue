<script setup lang="ts">
import { useAll, useDb, useSession } from "jazz-tools/vue";
import { app } from "../session-app.js";

  const db = useDb();

  // #region auth-session-vue-hook
  const session = useSession();
  // #endregion auth-session-vue-hook

  // #region auth-session-vue-user-id
  const sessionUserId = session?.user_id ?? null;
  // #endregion auth-session-vue-user-id

// #region auth-session-vue-query
const ownedTodos = useAll(app.todos.where({ ownerId: sessionUserId ?? "__no-session__" }));
// #endregion auth-session-vue-query

  // #region auth-session-vue-insert
  function addOwnedTodo(title: string) {
    if (!sessionUserId) return;

  db.insert(app.todos, {
    title,
    done: false,
    ownerId: sessionUserId,
  });
}
// #endregion auth-session-vue-insert

  void ownedTodos;
  void addOwnedTodo;
</script>

<template></template>
