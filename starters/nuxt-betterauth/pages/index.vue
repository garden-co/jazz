<template>
  <main class="page-center">
    <img src="/jazz.svg" alt="Jazz" class="wordmark" />
    <div class="card">
      <h1>{{ isSignUp ? "Create account" : "Sign in" }}</h1>
      <form @submit.prevent="handleSubmit">
        <div v-if="isSignUp" class="field">
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
          {{ isPending ? "Please wait…" : isSignUp ? "Create account" : "Sign in" }}
        </button>
      </form>
      <p class="toggle">
        {{ isSignUp ? "Already have an account?" : "New here?" }}
        <button
          type="button"
          class="link"
          @click="
            isSignUp = !isSignUp;
            error = null;
          "
        >
          {{ isSignUp ? "Sign in" : "Create an account" }}
        </button>
      </p>
    </div>
  </main>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { createAuthClient } from "better-auth/vue";

definePageMeta({ layout: "default" });

const authClient = createAuthClient();
const isSignUp = ref(false);
const name = ref("");
const email = ref("");
const password = ref("");
const error = ref<string | null>(null);
const isPending = ref(false);

async function handleSubmit() {
  error.value = null;
  isPending.value = true;
  try {
    const res = isSignUp.value
      ? await authClient.signUp.email({
          name: name.value,
          email: email.value,
          password: password.value,
        })
      : await authClient.signIn.email({ email: email.value, password: password.value });

    if (res.error) {
      error.value = res.error.message ?? (isSignUp.value ? "Sign-up failed" : "Sign-in failed");
      return;
    }
    await navigateTo("/dashboard");
  } finally {
    isPending.value = false;
  }
}
</script>
