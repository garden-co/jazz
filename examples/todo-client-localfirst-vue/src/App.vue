<script setup lang="ts">
import { shallowRef, watch } from "vue";
import { JazzProvider } from "jazz-tools/vue";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "vue-sonner";
import TodoList from "./TodoList.vue";
import { prepareAccountConfig } from "./account.js";

const props = defineProps<{ config?: Partial<DbConfig> }>();
const config = shallowRef<DbConfig>();
const error = shallowRef<Error>();
// #region context-setup-vue
watch(
  () => props.config,
  (overrides, _previous, onCleanup) => {
    let cancelled = false;
    onCleanup(() => {
      cancelled = true;
    });
    config.value = undefined;
    error.value = undefined;
    prepareAccountConfig(overrides).then(
      (value) => {
        if (!cancelled) config.value = value;
      },
      (cause) => {
        if (!cancelled) error.value = cause instanceof Error ? cause : new Error(String(cause));
      },
    );
  },
  { immediate: true },
);
// #endregion context-setup-vue
</script>

<template>
  <p v-if="error" role="alert">{{ error.message }}</p>
  <JazzProvider v-else-if="config" :config="config">
    <h1>Todos</h1>
    <TodoList />
    <Toaster />
    <template #fallback>
      <p>Loading...</p>
    </template>
  </JazzProvider>
  <p v-else>Loading...</p>
</template>
