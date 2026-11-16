<script setup lang="ts">
import { JazzProvider, type JazzClientConfig } from "jazz-tools/vue";
defineProps<{ config: JazzClientConfig }>();
</script>
<template>
  <JazzProvider :config="config"><slot /></JazzProvider>
</template>
