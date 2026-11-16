<!-- #region context-setup-vue-runtime-sources -->
<script setup lang="ts">
import { JazzProvider, type JazzClientConfig } from "jazz-tools/vue";

const config: JazzClientConfig = {
  serverUrl: "https://my-jazz-server.example.com",
  initial: "device",
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
