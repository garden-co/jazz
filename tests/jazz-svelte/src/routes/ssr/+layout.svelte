<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { beforeNavigate, goto } from "$app/navigation";
  import { co } from "jazz-tools";

  if (typeof window !== "undefined") {
    localStorage.clear();
  }

  const TestAccount = co.account({
    profile: co.profile(),
    root: co.map({}),
  });

  let { children } = $props();
</script>

<JazzSvelteProvider
  AccountSchema={TestAccount}
  sync={{ peer: "ws://localhost:4250/" }}
  navigation={{ beforeNavigate, goto }}
>
  {@render children()}
</JazzSvelteProvider>
