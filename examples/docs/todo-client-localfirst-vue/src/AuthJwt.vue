<script setup lang="ts">
import { JazzProvider } from "jazz-tools/vue";
// Your backend names the user (its authenticate reads your session cookie).
// After your own sign-in, call refresh() from useJazzSession() in the signedOut slot.
</script>
<template>
  <JazzProvider :config="{ serverUrl: '/jazz' }">
    <slot />
    <template #signedOut><slot name="signedOut" /></template>
  </JazzProvider>
</template>
