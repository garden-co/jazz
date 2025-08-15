<template>
  <div v-if="auth.state === 'signedIn'">
    <slot />
  </div>
  <div v-else :style="containerStyle">
    <div :style="cardStyle">
      <h1 :style="headingStyle">{{ appName }}</h1>
      
      <div v-if="error" :style="errorStyle">
        {{ error }}
      </div>

      <form @submit.prevent="handleSignUp" :style="formStyle">
        <input
          v-model="username"
          type="text"
          placeholder="Display name"
          autocomplete="name"
          :style="inputStyle"
        />
        <button type="submit" :style="primaryButtonStyle">
          Sign up
        </button>
      </form>

      <button @click="handleLogIn" :style="secondaryButtonStyle">
        Log in with existing account
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { usePasskeyAuth } from "./usePasskeyAuth.js";

interface Props {
  appName: string;
  appHostname?: string;
}

const props = defineProps<Props>();

const username = ref("");
const error = ref<string | null>(null);

const auth = usePasskeyAuth({
  appName: props.appName,
  appHostname: props.appHostname,
});

function handleError(err: Error) {
  if (err.cause instanceof Error) {
    error.value = err.cause.message;
  } else {
    error.value = err.message;
  }
}

const handleSignUp = async () => {
  if (!username.value.trim()) {
    error.value = "Name is required";
    return;
  }

  error.value = null;
  try {
    await auth.value.signUp(username.value.trim());
  } catch (err) {
    handleError(err as Error);
  }
};

const handleLogIn = async () => {
  error.value = null;
  try {
    await auth.value.logIn();
  } catch (err) {
    handleError(err as Error);
  }
};

// Styles (matching React version)
const containerStyle = {
  width: "100vw",
  height: "100vh",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  backgroundColor: "#f3f4f6",
  padding: "1rem",
};

const cardStyle = {
  backgroundColor: "white",
  padding: "2rem",
  borderRadius: "0.5rem",
  boxShadow: "0 10px 15px -3px rgba(0, 0, 0, 0.1)",
  width: "100%",
  maxWidth: "18rem",
  display: "flex",
  flexDirection: "column" as const,
  gap: "2rem",
};

const headingStyle = {
  fontSize: "1.5rem",
  fontWeight: "bold",
  textAlign: "center" as const,
  margin: "0",
};

const errorStyle = {
  color: "red",
  fontSize: "0.875rem",
  textAlign: "center" as const,
};

const formStyle = {
  display: "flex",
  flexDirection: "column" as const,
  gap: "0.5rem",
};

const inputStyle = {
  padding: "0.75rem",
  border: "1px solid #d1d5db",
  borderRadius: "0.375rem",
  fontSize: "1rem",
};

const primaryButtonStyle = {
  backgroundColor: "#3b82f6",
  color: "white",
  padding: "0.75rem 1rem",
  border: "none",
  borderRadius: "0.375rem",
  cursor: "pointer",
  fontSize: "1rem",
  fontWeight: "500",
};

const secondaryButtonStyle = {
  backgroundColor: "#e5e7eb",
  color: "#374151",
  padding: "0.75rem 1rem",
  border: "none",
  borderRadius: "0.375rem",
  cursor: "pointer",
  fontSize: "1rem",
  fontWeight: "500",
};
</script>
