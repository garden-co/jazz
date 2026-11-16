<script setup lang="ts">
import { useDb, useJazzSession } from "jazz-tools/vue";
import { app } from "../jazz.generated.js";

// #region db-access-vue
// Must be called inside setup() or <script setup>
const db = useDb();

async function addTodo(title: string) {
  await db.insert(app.todos, { title, done: false });
}
// #endregion db-access-vue

// #region session-vue
// Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null.
// Your permissions see the same user as `session.user` (the accountId),
// `session.identity` and `session.claims`.
const { snapshot } = useJazzSession(); // snapshot.value.user
// #endregion session-vue

void addTodo;
void snapshot;
</script>

<template></template>
