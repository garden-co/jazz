<script setup lang="ts">
import { computed } from "vue";
import { JazzProvider } from "jazz-tools/vue";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "vue-sonner";
import TodoList from "./TodoList.vue";

const props = defineProps<{ config?: Partial<DbConfig> }>();
// #region context-setup-vue
// The app's backend serves Jazz on this origin (`/jazz`), so the client needs no URL.
const config = computed(() => ({
  env: "dev" as const,
  ...props.config,
}));
// Explicit account overrides retain the caller-owned account path used by tests;
// otherwise the app starts with a local-first guest account.
const providerProps = computed(() =>
  config.value.account ? { config: config.value as DbConfig } : sessionProps(config.value),
);
// Tests point the app at a sync server by its app id; a browser client takes
// that as its `scope` (it names no app of its own).
function sessionProps({ appId, ...rest }: Partial<DbConfig>) {
  return {
    ...rest,
    ...(appId === undefined ? {} : { scope: appId }),
    initial: "local-first" as const,
  };
}
// #endregion context-setup-vue
</script>

<template>
  <JazzProvider v-bind="providerProps" :key="JSON.stringify([config.appId, config.serverUrl])">
    <h1>Todos</h1>
    <TodoList />
    <Toaster />
  </JazzProvider>
</template>
