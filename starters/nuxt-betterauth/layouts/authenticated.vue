<template>
  <JazzProvider v-if="jazzClient" :client="jazzClient">
    <JwtRefresh />
    <slot />
    <template #fallback>
      <p>Loading…</p>
    </template>
  </JazzProvider>
</template>

<script setup lang="ts">
import { JazzProvider, createJazzClient, type JazzClient } from "jazz-tools/vue";
import { createAuthClient } from "better-auth/vue";
import { jwtClient } from "better-auth/client/plugins";
import { shallowRef, onMounted } from "vue";
import JwtRefresh from "~/components/JwtRefresh.client.vue";

const config = useRuntimeConfig();
const jazzClient = shallowRef<Promise<JazzClient> | null>(null);

onMounted(async () => {
  const authClient = createAuthClient({ plugins: [jwtClient()] });
  const { data: session, error: sessionError } = await authClient.getSession();
  if (sessionError || !session) return;
  const { data: tokenData, error: tokenError } = await authClient.token();
  if (tokenError || !tokenData?.token) return;
  const appId = config.public.jazzAppId as string;
  const serverUrl = config.public.jazzServerUrl as string;
  jazzClient.value = createJazzClient({ appId, serverUrl, jwtToken: tokenData.token });
});
</script>
