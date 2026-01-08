<script lang="ts">
import type { Snippet } from "svelte";
import { createAuthClient } from "better-auth/client";
import { getAuthSecretStorage, getJazzContext } from "jazz-tools/svelte";
import { jazzPluginClient } from "./client.js";

type AuthClient = ReturnType<
  typeof createAuthClient<{
    plugins: [ReturnType<typeof jazzPluginClient>];
  }>
>;

let {
  betterAuthClient,
  children,
}: {
  betterAuthClient: AuthClient;
  children?: Snippet;
} = $props();

const context = getJazzContext();
const authSecretStorage = getAuthSecretStorage();

if (betterAuthClient.jazz === undefined) {
  throw new Error(
    "Better Auth client has been initialized without the jazzPluginClient",
  );
}

$effect(() => {
  // Register reactive dependencies
  context.current;
  authSecretStorage;
  betterAuthClient;

  // The plugin installs itself on the client under the `jazz` key:
  betterAuthClient.jazz.setJazzContext(context.current);
  betterAuthClient.jazz.setAuthSecretStorage(authSecretStorage);

  // If we don't subscribe, then the plugin won't keep the states synced, but we don't need to actually do anything in the callback.
  return betterAuthClient.useSession.subscribe(() => {});
});
</script>

{#if children}
  {@render children()}
{/if}
