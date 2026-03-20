<script setup lang="ts">
  // #region branch-vue
  import { createJazzClient, JazzProvider } from "jazz-tools/vue";

  const client = createJazzClient({
    appId: "my-app",
    env: "prod",
    userBranch: "staging",
  });
  // #endregion branch-vue
</script>

<template>
  <JazzProvider :client="client">
    <slot />
  </JazzProvider>
</template>
