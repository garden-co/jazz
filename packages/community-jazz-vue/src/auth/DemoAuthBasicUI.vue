<script setup lang="ts">
import { ref } from "vue";
import { useDemoAuth } from "./useDemoAuth.js";

interface Props {
  appName: string;
}

defineProps<Props>();
defineSlots<{
  // biome-ignore lint/suspicious/noExplicitAny: Complex type
  default?: () => any;
}>();

const auth = useDemoAuth();
const username = ref("");

const darkMode =
  typeof window !== "undefined"
    ? window.matchMedia("(prefers-color-scheme: dark)").matches
    : false;

const handleSubmit = (e: Event) => {
  e.preventDefault();
  auth.value.signUp(username.value);
};
</script>

<template>
  <slot v-if="auth.state === 'signedIn'" />
  <div
    v-else
    :style="{
      minHeight: '100%',
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'center',
      width: '18rem',
      maxWidth: 'calc(100vw - 2rem)',
      gap: '2rem',
      margin: '0 auto',
    }"
  >
    <h1
      :style="{
        color: darkMode ? '#fff' : '#000',
        textAlign: 'center',
        fontSize: '1.5rem',
        fontWeight: 'bold',
      }"
    >
      {{ appName }}
    </h1>

    <form
      :style="{
        display: 'flex',
        flexDirection: 'column',
        gap: '0.5rem',
      }"
      @submit="handleSubmit"
    >
      <input
        v-model="username"
        placeholder="Display name"
        autocomplete="webauthn"
        :style="{
          border: darkMode ? '1px solid #444' : '1px solid #ddd',
          padding: '11px 8px',
          borderRadius: '6px',
          background: darkMode ? '#000' : '#fff',
          color: darkMode ? '#fff' : '#000',
        }"
      />
      <input
        type="submit"
        value="Sign up"
        :style="{
          padding: '13px 5px',
          border: 'none',
          borderRadius: '6px',
          cursor: 'pointer',
          background: darkMode ? '#444' : '#ddd',
          color: darkMode ? '#fff' : '#000',
        }"
      />
    </form>

    <div
      v-if="auth.existingUsers.length > 0"
      :style="{
        display: 'flex',
        flexDirection: 'column',
        gap: '0.5rem',
      }"
    >
      <p
        :style="{
          color: darkMode ? '#e2e2e2' : '#000',
          textAlign: 'center',
          paddingTop: '0.5rem',
          borderTop: '1px solid',
          borderColor: darkMode ? '#111' : '#e2e2e2',
        }"
      >
        Log in as
      </p>
      <button
        v-for="user in auth.existingUsers"
        :key="user"
        @click="auth.logIn(user)"
        type="button"
        :aria-label="`Log in as ${user}`"
        :style="{
          background: darkMode ? '#0d0d0d' : '#eee',
          color: darkMode ? '#fff' : '#000',
          padding: '0.5rem',
          border: 'none',
          borderRadius: '6px',
        }"
      >
        {{ user }}
      </button>
    </div>
  </div>
</template>
