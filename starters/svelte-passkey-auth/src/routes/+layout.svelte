<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { apiKey } from "../apiKey";
  import "../app.css";
  import Header from "$lib/components/Header.svelte";
  import "jazz-tools/inspector/register-custom-element";
  import { JazzAccount } from "$lib/schema";

  let { children } = $props();
  let appName = "Jazz Svelte starter";
</script>

<svelte:head>
  <title>Jazz | Svelte + Tailwind</title>
</svelte:head>

<JazzSvelteProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  }}
  AccountSchema={JazzAccount}
>
  <jazz-inspector></jazz-inspector>
  <Header {appName} />
  <main class="max-w-2xl mx-auto px-3 mt-16 flex flex-col gap-8">
    {@render children?.()}
  </main>
</JazzSvelteProvider>
