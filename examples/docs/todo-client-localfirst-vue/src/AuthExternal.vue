<script setup lang="ts">
import { JazzProvider, useJazzSession, type JazzClient, type JazzSession } from "jazz-tools/vue";
// A session you opened with await createJazzClient({ serverUrl: "/jazz" }).
const props = defineProps<{ session: JazzSession<JazzClient> }>();
const { snapshot, refresh } = useJazzSession(props.session);
// After your own sign-in has set its session cookie, ask your backend who is signed in.
function signIn() {
  void refresh().catch(() => {});
}
</script>
<template>
  <JazzProvider :session="session">
    <slot />
    <template #signedOut>
      <button :disabled="!!snapshot.pending" @click="signIn">Sign in</button>
      <p v-if="snapshot.error" role="alert">{{ snapshot.error.message }}</p>
    </template>
  </JazzProvider>
</template>
