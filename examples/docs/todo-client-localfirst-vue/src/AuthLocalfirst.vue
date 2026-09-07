<script setup lang="ts">
import type { JazzSession, JazzClient } from "jazz-tools/vue";
import { JazzSessionProvider } from "jazz-tools/vue";
// Configure once with await createJazzSession({ appId, serverUrl, initial: "local-first" }).
defineProps<{ session: JazzSession<JazzClient> }>();
</script>
<template>
  <JazzSessionProvider :session="session"><slot /></JazzSessionProvider>
</template>
