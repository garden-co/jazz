<script setup lang="ts">
import {
  createJazzClient,
  JazzProvider,
} from "jazz-tools/vue";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";

const appId = "my-app";
const seed = loadOrCreateIdentitySeed(appId);
const jwtToken = mintSelfSignedToken(seed.seed, appId);

const client = createJazzClient({
  appId,
  serverUrl: "http://127.0.0.1:4200",
  jwtToken,
});
</script>

<template>
  <JazzProvider :client="client">
    <slot />
  </JazzProvider>
</template>
