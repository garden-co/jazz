<!-- #region query-subscription-vue -->
<script setup lang="ts">
  import { useAll } from "jazz-tools/vue";
  import { app } from "../schema/app.js";

  const todos = useAll(app.todos.where({ done: false }));
</script>

<template>
  <li v-for="todo in todos ?? []" :key="todo.id">{{ todo.title }}</li>
</template>
<!-- #endregion query-subscription-vue -->
