<template>
  <main class="page-center">
    <img src="/jazz.svg" alt="Jazz" class="wordmark" />
    <div class="card">
      <h1>Sign in</h1>
      <form @submit.prevent="handleSubmit">
        <div class="field">
          <label for="email">Email</label>
          <input id="email" v-model="email" type="email" required />
        </div>
        <div class="field">
          <label for="password">Password</label>
          <input id="password" v-model="password" type="password" required />
        </div>
        <p v-if="error" class="alert-error" role="alert">{{ error }}</p>
        <button type="submit" class="btn-primary" :disabled="isPending">
          {{ isPending ? "Signing in…" : "Sign in" }}
        </button>
      </form>
      <p class="toggle">New here? <NuxtLink to="/signup">Create an account</NuxtLink></p>
    </div>
  </main>
</template>

<script setup lang="ts">
import { ref } from "vue";

const authClient = useAuthClient();
const email = ref("");
const password = ref("");
const error = ref<string | null>(null);
const isPending = ref(false);

async function handleSubmit() {
  error.value = null;
  isPending.value = true;
  try {
    const res = await authClient.signIn.email({ email: email.value, password: password.value });
    if (res.error) {
      error.value = res.error.message ?? "Sign-in failed";
      return;
    }
    await navigateTo("/");
  } finally {
    isPending.value = false;
  }
}
</script>
