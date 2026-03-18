<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import AuthSessionExamples from "./AuthSessionExamples.vue";
import TodoList from "./TodoList.vue";

// Keep docs-only auth snippets in the compiled example app.
void AuthSessionExamples;

const client = createJazzClient({
  appId: "todo-vue-example",
  serverUrl: "http://127.0.0.1:1625",
  localAuthMode: "anonymous",
  // jwtToken: authToken, // Use this (instead of localAuthMode) for external auth.
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
