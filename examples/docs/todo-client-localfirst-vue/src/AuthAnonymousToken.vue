<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";

const client = createJazzClient({
  appId: "my-app",
  localAuthMode: "anonymous",
  localAuthToken: "device-token-123",
});
</script>

<template>
  <JazzProvider :client="client">
    <slot />
  </JazzProvider>
</template>
