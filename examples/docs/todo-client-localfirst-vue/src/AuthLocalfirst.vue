<script setup lang="ts">
import { ref, onMounted } from "vue";
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import { BrowserAuthSecretStore } from "jazz-tools";

const client = ref<ReturnType<typeof createJazzClient> | null>(null);

onMounted(async () => {
  const secret = await BrowserAuthSecretStore.getOrCreateSecret();
  client.value = createJazzClient({
    appId: "my-app",
    secret,
  });
});
</script>

<template>
  <JazzProvider v-if="client" :client="client">
    <slot />
  </JazzProvider>
</template>
