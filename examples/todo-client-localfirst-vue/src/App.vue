<script setup lang="ts">
import { computed } from "vue";
import { JazzProvider } from "jazz-tools/vue";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "vue-sonner";
import TodoList from "./TodoList.vue";

const props = defineProps<{ config?: Partial<DbConfig> }>();
// #region context-setup-vue
const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
const config = computed(() => ({
  appId: env?.VITE_JAZZ_APP_ID ?? env?.JAZZ_APP_ID,
  serverUrl: env?.VITE_JAZZ_SERVER_URL ?? env?.JAZZ_SERVER_URL,
  env: "dev" as const,
  ...props.config,
}));
// Explicit account overrides retain the caller-owned account path used by tests.
const providerProps = computed(() =>
  config.value.account ? { config: config.value as DbConfig } : config.value,
);
// #endregion context-setup-vue
</script>

<template>
  <JazzProvider v-bind="providerProps" :key="JSON.stringify([config.appId, config.serverUrl])">
    <h1>Todos</h1>
    <TodoList />
    <Toaster />
  </JazzProvider>
</template>
