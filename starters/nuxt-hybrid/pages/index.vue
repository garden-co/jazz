<template>
  <main class="page-center">
    <img src="/jazz.svg" alt="Jazz" class="wordmark" />
    <header
      style="width: 100%; max-width: 540px; display: flex; justify-content: flex-end; gap: 0.5rem"
    >
      <template v-if="sessionStore.data">
        <span style="color: var(--muted); align-self: center">{{
          sessionStore.data.user.email
        }}</span>
        <button type="button" @click="signOut">Sign out</button>
      </template>
      <template v-else>
        <NuxtLink to="/signin">Sign in</NuxtLink>
        <NuxtLink to="/signup">Sign up</NuxtLink>
      </template>
    </header>
    <TodoWidget />
    <AuthBackup v-if="!sessionStore.data" />
  </main>
</template>

<script setup lang="ts">
import { BrowserAuthSecretStore } from "jazz-tools";
import TodoWidget from "~/components/TodoWidget.client.vue";
import AuthBackup from "~/components/AuthBackup.client.vue";

const authClient = useAuthClient();
const sessionStore = authClient.useSession();

async function signOut() {
  await authClient.signOut();
  await BrowserAuthSecretStore.clearSecret();
  window.location.href = "/";
}
</script>
