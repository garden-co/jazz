<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import TodoList from "./TodoList.vue";

const client = createJazzClient({
  appId: "my-todo-app",
});
</script>

<template>
  <JazzProvider :client="client">
    <h1>Todos</h1>
    <TodoList />

    <template #fallback>
      <p>Loading...</p>
    </template>
  </JazzProvider>
</template>
