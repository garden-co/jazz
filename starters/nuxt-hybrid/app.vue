<template>
  <JazzProvider v-if="jazzClient" :client="jazzClient">
    <NuxtPage />
    <template #fallback>
      <p>Loading…</p>
    </template>
  </JazzProvider>
</template>

<script setup lang="ts">
import { JazzProvider, createJazzClient, type JazzClient } from "jazz-tools/vue";
import { BrowserAuthSecretStore } from "jazz-tools";
import { shallowRef, watch } from "vue";

const config = useRuntimeConfig();
const jazzClient = shallowRef<Promise<JazzClient> | null>(null);
const authClient = useAuthClient();
const sessionStore = authClient.useSession();
const appId = config.public.jazzAppId as string;
const serverUrl = config.public.jazzServerUrl as string;

watch(
  sessionStore,
  async (store) => {
    if (!appId || !serverUrl || store.isPending) return;
    if (store.data) {
      const { data, error } = await authClient.token();
      if (!error && data?.token) {
        jazzClient.value = createJazzClient({ appId, serverUrl, jwtToken: data.token });
        return;
      }
    }
    const secret = await BrowserAuthSecretStore.getOrCreateSecret();
    jazzClient.value = createJazzClient({ appId, serverUrl, secret });
  },
  { immediate: true },
);
</script>

<style>
@import "~/assets/main.css";
</style>
