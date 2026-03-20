<script setup lang="ts">
  import { ref } from "vue";
  import { createJazzClient, JazzProvider, useLinkExternalIdentity } from "jazz-tools/vue";

  const appId = "my-app";
  const serverUrl = "http://127.0.0.1:4200";
  const providerJwt = "<provider-jwt>";
  const hasJwt = ref(false);

  const linkExternalIdentity = useLinkExternalIdentity({
    appId,
    serverUrl,
    defaultMode: "anonymous",
  });

  const localClient = createJazzClient({
    appId,
    serverUrl,
  });

  const jwtClient = createJazzClient({
    appId,
    serverUrl,
    jwtToken: providerJwt,
  });

  async function onSignedIn() {
    await linkExternalIdentity({ jwtToken: providerJwt });
    hasJwt.value = true;
  }
</script>

<template>
  <JazzProvider :key="hasJwt ? 'jwt' : 'local'" :client="hasJwt ? jwtClient : localClient">
    <button type="button" @click="onSignedIn">Sign in</button>
    <slot />
  </JazzProvider>
</template>
