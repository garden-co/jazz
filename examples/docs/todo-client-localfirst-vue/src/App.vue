<script setup lang="ts">
import { JazzProvider, type JazzClientConfig } from "jazz-tools/vue";
import TodoList from "./TodoList.vue";

defineProps<{ config: JazzClientConfig }>();
</script>

<template>
  <JazzProvider :config="config">
    <h1>Todos</h1>
    <TodoList />

    <template #loading>
      <p>Loading...</p>
    </template>
  </JazzProvider>
</template>
