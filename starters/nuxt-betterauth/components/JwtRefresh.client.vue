<template></template>

<script setup lang="ts">
import { useDb } from "jazz-tools/vue";
import { onUnmounted } from "vue";
import { createAuthClient } from "better-auth/vue";
import { jwtClient } from "better-auth/client/plugins";

const db = useDb();
const authClient = createAuthClient({ plugins: [jwtClient()] });

const stop = db.onAuthChanged(async (state) => {
  if (state.status !== "unauthenticated") return;
  const { data, error } = await authClient.token();
  if (!error && data?.token) db.updateAuthToken(data.token);
});

onUnmounted(() => stop());
</script>
