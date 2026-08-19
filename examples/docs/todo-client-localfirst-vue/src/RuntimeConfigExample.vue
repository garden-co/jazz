<!-- #region context-setup-vue-runtime-sources -->
<script setup lang="ts">
import { JazzProvider } from "jazz-tools/vue";

const config = {
  appId: "my-app",
  serverUrl: "https://my-jazz-server.example.com",
  runtimeSources: {
    baseUrl: "/assets/jazz/",
  },
};
</script>

<template>
  <JazzProvider :config="config">
    <!-- ... -->
  </JazzProvider>
</template>
<!-- #endregion context-setup-vue-runtime-sources -->
