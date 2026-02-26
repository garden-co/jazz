<script lang="ts">
	import { onDestroy } from 'svelte';
	import type { Db } from '../runtime/db.js';
	import { initJazzContext } from './context.svelte.js';
	import type { JazzClient } from './create-jazz-client.js';

	interface Props {
		client: JazzClient | Promise<JazzClient>;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
	}

	let { client, children, fallback }: Props = $props();

	const ctx = initJazzContext();
	let resolvedClient: JazzClient | null = null;
	let error = $state<Error | null>(null);
	let cancelled = false;

	Promise.resolve(client)
		.then((c) => {
			if (cancelled) {
				void c.shutdown();
				return;
			}
			resolvedClient = c;
			ctx.db = c.db;
			ctx.session = c.session;
		})
		.catch((reason) => {
			error = reason instanceof Error ? reason : new Error(String(reason));
		});

	onDestroy(() => {
		cancelled = true;
		if (resolvedClient) {
			void resolvedClient.shutdown();
		}
	});
</script>

{#if error}
	<!-- Re-throw so an error boundary can catch it -->
	{(() => { throw error; })()}
{:else if ctx.db}
	{@render children({ db: ctx.db })}
{:else if fallback}
	{@render fallback()}
{/if}
