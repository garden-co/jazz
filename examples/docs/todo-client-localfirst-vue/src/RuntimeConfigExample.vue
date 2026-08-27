<!-- #region context-setup-vue-runtime-sources -->
<script setup lang="ts">
import { JazzProvider } from "jazz-tools/vue";

const config = {
  appId: "my-app",
  serverUrl: "https://my-jazz-server.example.com",
  runtimeSources: {
    baseUrl: "/assets/jazz/",
    wasmVersion: "2026-08-25", // Change this for every deployed asset build.
  },
};
</script>

<template>
  <JazzProvider :config="config">
    <!-- ... -->
  </JazzProvider>
</template>
<!-- #endregion context-setup-vue-runtime-sources -->
