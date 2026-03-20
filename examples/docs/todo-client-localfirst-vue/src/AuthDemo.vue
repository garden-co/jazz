<script setup lang="ts">
  import {
    createJazzClient,
    getActiveSyntheticAuth,
    JazzProvider,
    SyntheticUserSwitcher,
  } from "jazz-tools/vue";

  const appId = "my-app";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  const client = createJazzClient({
    appId,
    serverUrl: "http://127.0.0.1:4200",
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
  });
</script>

<template>
  <SyntheticUserSwitcher :app-id="appId" default-mode="demo" />
  <JazzProvider :client="client">
    <slot />
  </JazzProvider>
</template>
