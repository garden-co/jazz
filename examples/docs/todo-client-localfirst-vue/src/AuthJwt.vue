<script setup lang="ts">
import { JazzProvider } from "jazz-tools/vue";

const config = {
  appId: "my-app",
  serverUrl: "http://127.0.0.1:4200",
  jwtToken: "<provider-jwt>",
};
</script>

<template>
  <JazzProvider :config="config">
    <slot />
  </JazzProvider>
</template>
