<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { syncConfig } from "./sync-config";
  import { Account } from "jazz-tools";
  let { children } = $props();

  // Enable guest mode for account-less access
  const guestMode = false;

  // Default name for new user profiles
  const defaultProfileName = "New User";

  // Override the default storage key
  const authSecretStorageKey = "jazz-logged-in-secret";

  // Handle user logout
  const onLogOut = () => {
    console.log("User logged out");
  };

  // Handle anonymous account data when user logs in to existing account
  const onAnonymousAccountDiscarded = (account: Account) => {
    console.log("Anonymous account discarded", account.$jazz.id);
    // Migrate data here
    return Promise.resolve();
  };
</script>

<JazzSvelteProvider
  sync={syncConfig}
  {guestMode}
  {defaultProfileName}
  {onLogOut}
  {onAnonymousAccountDiscarded}
  {authSecretStorageKey}
>
  {@render children()}
</JazzSvelteProvider>
