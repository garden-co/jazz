<script setup lang="ts">
import { computed, ref } from "vue";
import { JazzProvider } from "jazz-tools/vue";

const appId = "my-app";
const serverUrl = "http://127.0.0.1:4200";
const providerJwt = "<provider-jwt>";
const hasJwt = ref(false);

const config = computed(() => ({
  appId,
  serverUrl,
  ...(hasJwt.value ? { jwtToken: providerJwt } : {}),
}));

function onSignedIn() {
  hasJwt.value = true;
}
</script>

<template>
  <JazzProvider :config="config">
    <button type="button" @click="onSignedIn">Sign in</button>
    <slot />
  </JazzProvider>
</template>
