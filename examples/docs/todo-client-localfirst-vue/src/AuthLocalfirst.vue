<script setup lang="ts">
import { computed } from "vue";
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/vue";

const { secret, isLoading } = useLocalFirstAuth();

const config = computed(() =>
  !isLoading.value && secret.value ? { appId: "my-app", secret: secret.value } : null,
);
</script>

<template>
  <JazzProvider v-if="config" :config="config">
    <slot />
  </JazzProvider>
</template>
