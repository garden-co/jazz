<script setup lang="ts">
import type { JazzSession, JazzClient } from "jazz-tools/vue";
import { JazzSessionProvider } from "jazz-tools/vue";
// Configure once with await createJazzClient({ serverUrl, initial: "local-first", client }).
defineProps<{ session: JazzSession<JazzClient> }>();
</script>
<template>
  <JazzSessionProvider :session="session"><slot /></JazzSessionProvider>
</template>
