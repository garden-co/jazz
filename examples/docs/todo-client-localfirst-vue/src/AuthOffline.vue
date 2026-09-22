<script setup lang="ts">
import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/vue";
// Prepare a handle outside this context with createAccountManager.
defineProps<{ config: DbConfig }>();
</script>
<template>
  <JazzProvider :config="{ ...config, serverUrl: undefined }"><slot /></JazzProvider>
</template>
