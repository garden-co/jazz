<!-- #region auth-session-vue -->
<script setup lang="ts">
import { useDb, useSession } from "jazz-tools/vue";
import { app } from "../schema/session-app.js";

const db = useDb();
const session = useSession();

async function loadOwnedTodos() {
  if (!session) return [];
  return db.all(app.todos.where({ owner_id: session.user_id }));
}

function addOwnedTodo(title: string) {
  if (!session) return;

  db.insert(app.todos, {
    title,
    done: false,
    owner_id: session.user_id,
  });
}
</script>

<template></template>
<!-- #endregion auth-session-vue -->
