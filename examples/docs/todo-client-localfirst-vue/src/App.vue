<script setup lang="ts">
import { computed } from "vue";
import { createJazzClient, JazzProvider, useLocalFirstAuth } from "jazz-tools/vue";
import TodoList from "./TodoList.vue";

const { secret, isLoading } = useLocalFirstAuth();
const client = computed(() =>
  !isLoading.value && secret.value
    ? createJazzClient({ appId: "<your-app-id>", secret: secret.value })
    : null,
);
</script>

<template>
  <JazzProvider v-if="client" :client="client">
    <h1>Todos</h1>
    <TodoList />

    <template #fallback>
      <p>Loading...</p>
    </template>
  </JazzProvider>
</template>
