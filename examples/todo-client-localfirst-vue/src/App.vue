<script setup lang="ts">
import { computed } from "vue";
import { JazzProvider, type JazzClientConfig } from "jazz-tools/vue";
import type { AppConfig } from "./app-config.js";
import { Toaster } from "vue-sonner";
import TodoList from "./TodoList.vue";

const props = defineProps<{ config?: AppConfig }>();
// #region context-setup-vue
// The app's backend serves Jazz on this origin (`/jazz`), so the client needs no URL.
const config = computed(() => ({
  env: "dev" as const,
  ...props.config,
}));
// The app starts as a device account (a guest whose key lives on this device).
// Tests point the app at a sync server by its app id; a browser client takes
// that as its `scope` (it names no app of its own).
const sessionConfig = computed((): JazzClientConfig => {
  const { appId, ...rest } = config.value;
  return {
    ...rest,
    ...(appId === undefined ? {} : { scope: appId }),
    initial: "device",
  };
});
// #endregion context-setup-vue
</script>

<template>
  <JazzProvider :config="sessionConfig" :key="JSON.stringify([config.appId, config.serverUrl])">
    <h1>Todos</h1>
    <TodoList />
    <Toaster />
  </JazzProvider>
</template>
