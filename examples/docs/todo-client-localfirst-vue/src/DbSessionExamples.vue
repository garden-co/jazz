<script setup lang="ts">
import { useDb, useJazzSession, useSession } from "jazz-tools/vue";
import { app } from "../schema.js";

// #region db-access-vue
// Must be called inside setup() or <script setup>
const db = useDb();

async function addTodo(title: string) {
  await db.insert(app.todos, { title, done: false });
}
// #endregion db-access-vue

// #region session-vue
// Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null
const { snapshot } = useJazzSession(); // snapshot.value.user
// What your permissions see as `session` (its `user` is the account id).
const session = useSession(); // ComputedRef<Session | null>
// #endregion session-vue

void addTodo;
void snapshot;
void session;
</script>

<template></template>
