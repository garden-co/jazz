<script setup lang="ts">
import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/vue";
import TodoList from "./TodoList.vue";

defineProps<{ config: DbConfig }>();
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
