<script lang="ts">
  import { ClerkProvider } from "svelte-clerk";
  // @ts-expect-error This is available in a real app.
  import { PUBLIC_CLERK_PUBLISHABLE_KEY } from "$env/static/public";
  import JazzClerkWrapper from "./JazzClerkWrapper.svelte";

  let { children } = $props();
</script>

<ClerkProvider publishableKey={PUBLIC_CLERK_PUBLISHABLE_KEY}>
  <JazzClerkWrapper>
    {@render children?.()}
  </JazzClerkWrapper>
</ClerkProvider>
