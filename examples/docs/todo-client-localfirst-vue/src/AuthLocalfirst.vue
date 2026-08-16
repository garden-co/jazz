<script setup lang="ts">
import { computed } from "vue";
import { createJazzClient, JazzProvider, useLocalFirstAuth } from "jazz-tools/vue";

const { secret, isLoading } = useLocalFirstAuth();

const client = computed(() =>
  !isLoading.value && secret.value
    ? createJazzClient({ appId: "my-app", secret: secret.value })
    : null,
);
</script>

<template>
  <JazzProvider v-if="client" :client="client">
    <slot />
  </JazzProvider>
</template>
