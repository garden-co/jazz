<script setup lang="ts">
import { useDb, useSession } from "jazz-tools/vue";
import { app } from "../schema.js";

// #region db-access-vue
// Must be called inside setup() or <script setup>
const db = useDb();

async function addTodo(title: string) {
  await db.insert(app.todos, { title, done: false });
}
// #endregion db-access-vue

// #region session-vue
const session = useSession(); // ShallowRef<{ user_id: string } | null>
// #endregion session-vue

void addTodo;
void session;
</script>

<template></template>
