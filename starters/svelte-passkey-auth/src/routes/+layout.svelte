<script lang="ts">
import { JazzProvider } from "jazz-svelte";
import { apiKey } from "../apiKey";
import "../app.css";
import Header from "$lib/components/Header.svelte";
import "jazz-inspector-element";

let { children } = $props();
let appName = "Jazz Svelte starter";
</script>

<JazzProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`
  }}
>
  <jazz-inspector></jazz-inspector>
  <Header {appName} />
  <main class="mt-16 flex flex-col gap-8">
    {@render children?.()}
  </main>
</JazzProvider>
