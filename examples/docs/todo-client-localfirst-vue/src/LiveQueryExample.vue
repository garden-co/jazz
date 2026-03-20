<!-- #region reading-loading-state-vue -->
<script setup lang="ts">
import { useAll } from "jazz-tools/vue";
import { app } from "../schema/app.js";

const todos = useAll(app.todos);
// undefined = not yet connected; [] = connected, no rows; [...] = rows present
</script>

<template>
  <p v-if="todos === undefined">Connecting…</p>
  <ul v-else>
    <li v-for="todo in todos" :key="todo.id">{{ todo.title }}</li>
  </ul>
</template>
<!-- #endregion reading-loading-state-vue -->
