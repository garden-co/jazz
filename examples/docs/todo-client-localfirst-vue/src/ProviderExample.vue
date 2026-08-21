<script setup lang="ts">
import { JazzProvider } from "jazz-tools/vue";

const config = {
  appId: "my-app",
};
</script>

<template>
  <JazzProvider :config="config">
    <slot />
    <template #fallback>
      <p>Loading...</p>
    </template>
  </JazzProvider>
</template>
