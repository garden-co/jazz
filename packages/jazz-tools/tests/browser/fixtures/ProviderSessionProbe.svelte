<script lang="ts">
	import JazzSvelteProvider from '../../../src/svelte/JazzSvelteProvider.svelte';
	import type { JazzClient } from '../../../src/svelte/create-jazz-client.js';
	import ProviderSessionConsumer from './ProviderSessionConsumer.svelte';

	let { client }: { client: JazzClient | Promise<JazzClient> } = $props();
</script>

<JazzSvelteProvider {client}>
	{#snippet children({ db })}
		<ProviderSessionConsumer {db} />
	{/snippet}
</JazzSvelteProvider>
