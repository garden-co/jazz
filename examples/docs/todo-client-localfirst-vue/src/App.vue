<script setup lang="ts">
import { JazzProvider } from "jazz-tools/vue";
import TodoList from "./TodoList.vue";

const config = {
  appId: "<your-app-id>",
};
</script>

<template>
  <JazzProvider :config="config">
    <h1>Todos</h1>
    <TodoList />

    <template #fallback>
      <p>Loading...</p>
    </template>
  </JazzProvider>
</template>
