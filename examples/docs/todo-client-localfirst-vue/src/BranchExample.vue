<script setup lang="ts">
// #region branch-vue
import { JazzProvider } from "jazz-tools/vue";

const config = {
  appId: "my-app",
  env: "prod",
  userBranch: "staging",
};
// #endregion branch-vue
</script>

<template>
  <JazzProvider :config="config">
    <slot />
  </JazzProvider>
</template>
