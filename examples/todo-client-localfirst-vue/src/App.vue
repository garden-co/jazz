<script setup lang="ts">
import { computed } from "vue";
import { JazzProvider, createJazzClient } from "jazz-tools/vue";
import { generateAuthSecret, type DbConfig } from "jazz-tools";
import { Toaster } from "vue-sonner";
import TodoList from "./TodoList.vue";

interface Props {
  config?: Partial<DbConfig>;
}

const props = withDefaults(defineProps<Props>(), {
  config: () => ({}),
});

function readEnv(name: string): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env?.[name];
}

function secretStorageKey(appId: string): string {
  return `jazz-auth-secret:${encodeURIComponent(appId)}`;
}

function getOrCreateSecretSync(appId: string): string {
  const stored = localStorage.getItem(secretStorageKey(appId));
  if (stored) return stored;
  const secret = generateAuthSecret();
  localStorage.setItem(secretStorageKey(appId), secret);
  return secret;
}

// #region context-setup-vue
function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? readEnv("VITE_JAZZ_APP_ID");
  const serverUrl = overrides.serverUrl ?? readEnv("VITE_JAZZ_SERVER_URL");
  if (!appId)
    throw new Error("Missing appId: add jazzPlugin() to vite.config.ts or set VITE_JAZZ_APP_ID");
  const secret = overrides.auth?.localFirstSecret ?? getOrCreateSecretSync(appId);

  return {
    appId,
    env: "dev",
    userBranch: "main",
    auth: { localFirstSecret: secret },
    ...(serverUrl ? { serverUrl } : {}),
    ...overrides,
  };
}
// #endregion context-setup-vue

const client = computed(() => createJazzClient(defaultConfig(props.config)));
</script>

<template>
  <JazzProvider :client="client">
    <h1>Todos</h1>
    <TodoList />
    <Toaster />
    <template #fallback>
      <p>Loading...</p>
    </template>
  </JazzProvider>
</template>
