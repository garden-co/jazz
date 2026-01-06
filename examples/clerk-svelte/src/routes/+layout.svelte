<script lang="ts">
  import 'jazz-tools/inspector/register-custom-element';
  import { ClerkProvider, SignOutButton } from 'svelte-clerk';
  import { browser } from '$app/environment';
  import '../app.css';
  import JazzClerkWrapper from '$lib/components/JazzClerkWrapper.svelte';

  const PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

  if (!PUBLISHABLE_KEY) {
    throw new Error('Add your Clerk publishable key to the .env.local file');
  }

  const { children } = $props();

  // Check query param using window.location to avoid reactive dependency issues
  // during Clerk initialization. Initialize and update in effect to avoid state mutation errors.
  let isExpirationTest = $state(false);

  if (browser) {
    $effect(() => {
      const checkExpirationTest = () => {
        const params = new URLSearchParams(window.location.search);
        isExpirationTest = params.has('expirationTest');
      };

      // Check immediately
      checkExpirationTest();

      // Update when URL changes
      window.addEventListener('popstate', checkExpirationTest);
      return () => window.removeEventListener('popstate', checkExpirationTest);
    });
  }
</script>

<svelte:head>
  <title>Minimal Auth Clerk Example | Jazz</title>
</svelte:head>

<ClerkProvider publishableKey={PUBLISHABLE_KEY} afterSignOutUrl="/">
  {#if isExpirationTest}
    <!-- Route to test that when the Clerk user expires, the app is logged out -->
    <div class="container">
      <SignOutButton>Simulate expiration</SignOutButton>
    </div>
  {:else}
    <JazzClerkWrapper>
      {@render children?.()}
    </JazzClerkWrapper>
    <jazz-inspector></jazz-inspector>
  {/if}
</ClerkProvider>

<style>
  :global(html, body) {
    margin: 0;
    height: 100%;
  }
</style>
