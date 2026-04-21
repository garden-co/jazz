<!-- #region context-setup-vue-runtime-sources -->
<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";

const client = createJazzClient({
  appId: "my-app",
  serverUrl: "https://my-jazz-server.example.com",
  runtimeSources: {
    baseUrl: "/assets/jazz/",
  },
});
</script>

<template>
  <JazzProvider :client="client">
    <!-- ... -->
  </JazzProvider>
</template>
<!-- #endregion context-setup-vue-runtime-sources -->
