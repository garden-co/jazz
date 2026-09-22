<!-- #region context-setup-vue-runtime-sources -->
<script setup lang="ts">
import { computed } from "vue";
import type { AccountHandle } from "jazz-tools";
import { JazzProvider } from "jazz-tools/vue";

const props = defineProps<{ account: AccountHandle }>();
// Prepare the handle with these same runtimeSources.
const config = computed(() => ({
  account: props.account,
  appId: "my-app",
  serverUrl: "https://my-jazz-server.example.com",
  runtimeSources: {
    baseUrl: "/assets/jazz/",
    wasmVersion: "2026-08-25", // Change this for every deployed asset build.
  },
}));
</script>

<template>
  <JazzProvider :config="config">
    <!-- ... -->
  </JazzProvider>
</template>
<!-- #endregion context-setup-vue-runtime-sources -->
