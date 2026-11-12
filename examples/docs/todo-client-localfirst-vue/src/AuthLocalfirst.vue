<script setup lang="ts">
import { JazzProvider, type JazzClient, type JazzSession } from "jazz-tools/vue";
// Open once with await createJazzClient({ serverUrl, initial: "device", client }).
defineProps<{ session: JazzSession<JazzClient> }>();
</script>
<template>
  <JazzProvider :session="session"><slot /></JazzProvider>
</template>
