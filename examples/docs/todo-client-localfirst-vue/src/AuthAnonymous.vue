<script setup lang="ts">
  import { createJazzClient, JazzProvider } from "jazz-tools/vue";

  const client = createJazzClient({
    appId: "my-app",
    serverUrl: "http://127.0.0.1:4200",
  });
</script>

<template>
  <JazzProvider :client="client">
    <slot />
  </JazzProvider>
</template>
