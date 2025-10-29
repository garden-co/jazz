<script lang="ts">
  // @ts-expect-error Only in a real SK app
	import { page } from '$app/state';
	import Festival from '$lib/components/Festival.svelte';
	import NewBand from '$lib/components/NewBand.svelte';
	const { festivalId } = page.params;
</script>

<main>
  <h1>🎪 Festival {festivalId}</h1>
  <Festival id={festivalId} />
  <NewBand id={festivalId} />
</main>
