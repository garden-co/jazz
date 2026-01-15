<script lang="ts">
  // @ts-ignore svelte-clerk is not installed in the homepage project but it is in the monorepo
  import { ClerkProvider } from "svelte-clerk";
  // @ts-expect-error This is available in a real app.
  import { PUBLIC_CLERK_PUBLISHABLE_KEY } from "$env/static/public";
  import JazzClerkWrapper from "$lib/components/JazzClerkWrapper.svelte";

  let { children } = $props();
</script>

<ClerkProvider publishableKey={PUBLIC_CLERK_PUBLISHABLE_KEY}>
  <JazzClerkWrapper>
    {@render children?.()}
  </JazzClerkWrapper>
</ClerkProvider>
