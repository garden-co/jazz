<script setup lang="ts">
import { computed } from "vue";
import App from "../App.vue";
import ExpirationTest from "./ExpirationTest.vue";
import JazzProvider from "./JazzProvider.vue";

const isExpirationTest = computed(() =>
  location.search.includes("expirationTest"),
);
</script>

<template>
  <ExpirationTest v-if="isExpirationTest" />
  <JazzProvider v-else>
    <App />
  </JazzProvider>
</template>
