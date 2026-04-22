<template>
  <main class="page-center">
    <img src="/jazz.svg" alt="Jazz" class="wordmark" />
    <TodoWidget />
    <AuthBackup />
  </main>
</template>

<script setup lang="ts">
import TodoWidget from "~/components/TodoWidget.client.vue";
import AuthBackup from "~/components/AuthBackup.client.vue";
</script>
