<script setup lang="ts">
import { SignInButton, SignOutButton } from "@clerk/vue";
import { useIsAuthenticated, useJazzContext } from "community-jazz-vue";
import { Account } from "jazz-tools";
import { computed } from "vue";

const context = useJazzContext<Account>();
const isAuthenticated = useIsAuthenticated();

const me = computed(() => {
  const ctx = context.value;
  console.log("[App] me computed:", { hasContext: !!ctx, ctx });
  if (!ctx) return null;
  return "me" in ctx ? ctx.me : null;
});
</script>

<template>
  <div v-if="isAuthenticated" class="container">
    <h1>You're logged in</h1>
    <p>Welcome back, {{ me?.profile?.name || "User" }}</p>
    <SignOutButton>Logout</SignOutButton>
  </div>
  <div v-else class="container">
    <h1>You're not logged in</h1>
    <SignInButton />
  </div>
</template>

<style scoped>
.container {
  max-width: 600px;
  margin: 0 auto;
  padding: 2rem;
  text-align: center;
}
</style>
