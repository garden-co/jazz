<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";

const appId = "my-app";
const serverUrl = "http://127.0.0.1:4200";
const providerJwt = "<provider-jwt>";

const jwtClient = createJazzClient({
  appId,
  serverUrl,
  jwtToken: providerJwt,
});
</script>

<template>
  <JazzProvider :client="jwtClient">
    <slot />
  </JazzProvider>
</template>
