<script lang="ts">
  import { AccountCoState, useIsAuthenticated } from 'jazz-tools/svelte';
  import { Account } from 'jazz-tools';
  import { SignInButton, SignOutButton } from 'svelte-clerk';

  const account = new AccountCoState(Account, { resolve: { profile: true } });
  const me = $derived(account.current);
  const isAuthenticatedState = useIsAuthenticated();
  const isAuthenticated = $derived(isAuthenticatedState.current);
</script>

<div class="container">
  {#if isAuthenticated && me.$isLoaded}
    <h1>You're logged in</h1>
    <p>Welcome back, {me.profile.name}</p>
    <SignOutButton>Logout</SignOutButton>
  {:else}
    <h1>You're not logged in</h1>
    <SignInButton />
  {/if}
</div>

