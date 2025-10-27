<script lang="ts">
  // @ts-expect-error This is not a real SvelteKit app
  import { page } from "$app/state";
  import { createSSRJazzAgent } from "jazz-tools/ssr";
  import { TodoItem } from "./schema";
  // [!code hide]
  const apiKey = "";

  const itemId = $derived(page.params.itemId);
  // In order to avoid creating a new agent for every page, if you have more than one page which will be rendered server-side, the agent can be exported from a single centralised module.
  const jazzSSR = createSSRJazzAgent({
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  });
  const item = $derived(
    TodoItem.load(itemId, {
      loadAs: jazzSSR,
    }),
  );
</script>

<div class="flex flex-col items-center justify-center h-screen gap-4">
  <h1 class="text-2xl font-bold">SSR rendering example with Jazz</h1>
  <div class="text-sm text-gray-500 w-1/2 text-center">
    This component can render on the server!
  </div>
  <div class="text-sm">
    Item title
    {#await item then item}
      "{item.$isLoaded ? item.title : ""}"
    {/await}
  </div>
</div>
