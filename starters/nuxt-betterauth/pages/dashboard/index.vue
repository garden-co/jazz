<template>
  <main class="dashboard">
    <header>
      <img src="/jazz.svg" alt="Jazz" class="wordmark" />
      <div class="auth-nav">
        <p v-if="sessionStore.data">Hello, {{ sessionStore.data.user.name }}</p>
        <button type="button" @click="signOut">Sign out</button>
      </div>
    </header>
    <TodoWidget />
  </main>
</template>

<script setup lang="ts">
import { createAuthClient } from "better-auth/vue";
import TodoWidget from "~/components/TodoWidget.client.vue";

definePageMeta({ layout: "authenticated", middleware: ["auth"] });

const authClient = createAuthClient();
const sessionStore = authClient.useSession();

async function signOut() {
  await authClient.signOut();
  window.location.assign("/");
}
</script>
