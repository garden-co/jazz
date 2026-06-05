<script setup lang="ts">
import { computed } from "vue";
import { useAll, useDb, useSession } from "jazz-tools/vue";
import { app } from "../session-app.js";

const db = useDb();

// #region auth-session-vue-hook
const session = useSession();
// #endregion auth-session-vue-hook

// #region auth-session-vue-user-id
const sessionUserId = computed(() => session.value?.user_id ?? null);
// #endregion auth-session-vue-user-id

// #region auth-session-vue-query
const { data: ownedTodos } = useAll(
  computed(() =>
    sessionUserId.value ? app.todos.where({ owner_id: sessionUserId.value }) : undefined,
  ),
);
// #endregion auth-session-vue-query

// #region auth-session-vue-insert
function addOwnedTodo(title: string) {
  if (!sessionUserId.value) return;

  db.insert(app.todos, {
    title,
    done: false,
    owner_id: sessionUserId.value,
  });
}
// #endregion auth-session-vue-insert

void ownedTodos;
void addOwnedTodo;
</script>

<template></template>
