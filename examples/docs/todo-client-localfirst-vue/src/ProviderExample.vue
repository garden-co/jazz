<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";

const client = createJazzClient({
  appId: "my-app",
});
</script>

<template>
  <JazzProvider :client="client">
    <slot />
    <template #fallback>
      <p>Loading...</p>
    </template>
  </JazzProvider>
</template>
