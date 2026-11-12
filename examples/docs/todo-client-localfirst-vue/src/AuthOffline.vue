<script setup lang="ts">
import { JazzProvider, type JazzClientConfig } from "jazz-tools/vue";
// No server: Jazz stays on this device, with one device account per browser profile.
const config: JazzClientConfig = {
  serverUrl: null,
  client: () => import("jazz-tools/local-first"),
};
</script>
<template>
  <JazzProvider :config="config"><slot /></JazzProvider>
</template>
