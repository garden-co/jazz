<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import TodoList from "./TodoList.vue";

const client = createJazzClient({
  appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
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
