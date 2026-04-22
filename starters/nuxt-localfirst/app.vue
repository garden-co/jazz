<template>
  <JazzProvider v-if="jazzClient" :client="jazzClient">
    <NuxtPage />
    <template #fallback>
      <p>Loading…</p>
    </template>
  </JazzProvider>
</template>

<script setup lang="ts">
import { JazzProvider, createJazzClient } from "jazz-tools/vue";
import { BrowserAuthSecretStore } from "jazz-tools";
import { shallowRef, onMounted } from "vue";

const config = useRuntimeConfig();
const jazzClient = shallowRef<ReturnType<typeof createJazzClient> | null>(null);

onMounted(async () => {
  const appId = config.public.jazzAppId as string;
  const serverUrl = config.public.jazzServerUrl as string;
  if (!appId || !serverUrl) {
    const missing = [
      !appId && "NUXT_PUBLIC_JAZZ_APP_ID",
      !serverUrl && "NUXT_PUBLIC_JAZZ_SERVER_URL",
    ]
      .filter(Boolean)
      .join(" & ");
    console.error(`${missing} not set — the jazzNuxt() plugin should inject these.`);
    return;
  }
  const secret = await BrowserAuthSecretStore.getOrCreateSecret();
  jazzClient.value = createJazzClient({ appId, serverUrl, secret });
});
</script>

<style>
@import "~/assets/main.css";
</style>
