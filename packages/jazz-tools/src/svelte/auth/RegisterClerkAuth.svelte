<!--
  @component
  Internal component that registers the Clerk auth listener.

  This component exists because `useClerkAuth` requires access to the Jazz context,
  which is only available inside `JazzSvelteProvider`'s children. By placing the
  hook call in this child component, we ensure the context is properly initialized.
-->
<script lang="ts">
  import type { MinimalClerkClient } from "jazz-tools";
  import type { Snippet } from "svelte";
  import { useClerkAuth } from "./ClerkAuth.svelte.js";

  interface Props {
    clerk: MinimalClerkClient;
    children?: Snippet;
  }

  let { clerk, children }: Props = $props();

  // Register the Clerk auth listener after JazzSvelteProvider context is available.
  // The return value (auth state) is intentionally unused here - this component's
  // sole purpose is to register the listener that syncs Clerk and Jazz auth state.
  useClerkAuth(clerk);
</script>

{@render children?.()}
