<script lang="ts">
  import { jazzSSR } from "$lib/jazzSSR";
  import { Festival } from "$lib/schema";
  // @ts-expect-error Only available in a real SK app
  import { page } from '$app/state';

	const festivalId = $derived(page.params.festivalId);

  const festival = $derived(Festival.load(festivalId, {
    loadAs: jazzSSR,
    resolve: {
      $each: {
        $onError: 'catch',
      },
    },
  }));
</script>

<main>
  <h1>🎪 Server-rendered Festival {festivalId}</h1>
  <ul>
    {#await festival then festival}
      {#each festival.$isLoaded ? festival : [] as band (band.$jazz.id)}
        {#if band.$isLoaded}
          <li>🎶 {band.name}</li>
        {/if}
      {/each}
    {/await}
  </ul>
</main>
