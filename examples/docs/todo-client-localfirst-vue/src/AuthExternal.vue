<script setup lang="ts">
import { createAccountManager, type DbConfig } from "jazz-tools";
import { JazzProvider, useAccountState } from "jazz-tools/vue";
const props = defineProps<{
  accounts: Awaited<ReturnType<typeof createAccountManager>>;
  getToken: () => Promise<string>;
  config: Omit<DbConfig, "account">;
}>();
const state = useAccountState(props.accounts);
// Login starts without a context. Before linking, finish the old context's
// shutdown({ waitForSync: true }), then call accounts.linkJWT outside it.
function signIn() {
  void props.accounts.loginJWT({ getToken: props.getToken }).catch(() => {});
}
</script>
<template>
  <JazzProvider v-if="state.account" :config="{ ...config, account: state.account }"
    ><slot
  /></JazzProvider>
  <template v-else>
    <button :disabled="!!state.pending" @click="signIn">Sign in</button>
    <p v-if="state.error" role="alert">{{ state.error.message }}</p>
  </template>
</template>
