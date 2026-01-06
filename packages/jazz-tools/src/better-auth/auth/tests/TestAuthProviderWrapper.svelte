<script lang="ts">
import { JazzSvelteProvider } from "jazz-tools/svelte";
import AuthProvider from "../svelte.svelte";
import { createAuthClient } from "better-auth/client";
import { jazzPluginClient } from "../client";

type AuthClient = ReturnType<
  typeof createAuthClient<{
    plugins: [ReturnType<typeof jazzPluginClient>];
  }>
>;

let { betterAuthClient }: { betterAuthClient: AuthClient } = $props();
</script>

<JazzSvelteProvider
  storage={["memory"] as any}
  sync={{ peer: "ws://", when: "never" }}
>
  <AuthProvider {betterAuthClient}>
    <div></div>
  </AuthProvider>
</JazzSvelteProvider>
