<template>
  <main class="page-center">
    <img src="/jazz.svg" alt="Jazz" class="wordmark" />
    <div class="card">
      <h1>Create account</h1>
      <form @submit.prevent="handleSubmit">
        <div class="field">
          <label for="name">Name</label>
          <input id="name" v-model="name" type="text" required />
        </div>
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
          {{ isPending ? "Creating account…" : "Create account" }}
        </button>
      </form>
      <p class="toggle">Already have an account? <NuxtLink to="/signin">Sign in</NuxtLink></p>
    </div>
  </main>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { useDb } from "jazz-tools/vue";

const authClient = useAuthClient();
const db = useDb();
const name = ref("");
const email = ref("");
const password = ref("");
const error = ref<string | null>(null);
const isPending = ref(false);

async function handleSubmit() {
  error.value = null;
  isPending.value = true;
  try {
    const proofToken = await db.getLocalFirstIdentityProof({
      ttlSeconds: 60,
      audience: "nuxt-localfirst-signup",
    });
    if (!proofToken) {
      error.value = "Sign up requires an active Jazz session";
      return;
    }

    const res = await authClient.signUp.email({
      name: name.value,
      email: email.value,
      password: password.value,
      proofToken,
    } as Parameters<typeof authClient.signUp.email>[0]);

    if (res.error) {
      error.value = res.error.message ?? "Sign-up failed";
      return;
    }

    await navigateTo("/");
  } finally {
    isPending.value = false;
  }
}
</script>
