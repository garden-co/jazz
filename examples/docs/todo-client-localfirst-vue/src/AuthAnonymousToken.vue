<script setup lang="ts">
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";

const appId = "my-app";
const seed = loadOrCreateIdentitySeed(appId);
const jwtToken = mintSelfSignedToken(seed.seed, appId);

const client = createJazzClient({
  appId,
  jwtToken,
});
</script>

<template>
  <JazzProvider :client="client">
    <slot />
  </JazzProvider>
</template>
