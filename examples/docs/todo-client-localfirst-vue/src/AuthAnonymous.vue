<script setup lang="ts">
import { JazzProvider } from "jazz-tools/vue";
// Everyone starts as a guest: a device account, created on this device.
// Your backend accepts guests with acceptDeviceAccounts.
</script>
<template>
  <JazzProvider :config="{ serverUrl: '/jazz', initial: 'device' }"><slot /></JazzProvider>
</template>
