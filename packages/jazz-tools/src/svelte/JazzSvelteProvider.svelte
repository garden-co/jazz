<script lang="ts">
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
	let error = $state<Error | null>(null);

	$effect(() => {
		let cancelled = false;
		let resolved: JazzClient | null = null;

		Promise.resolve(client)
			.then((c) => {
				if (cancelled) {
					c.shutdown();
					return;
				}
				resolved = c;
				ctx.db = c.db;
				ctx.session = c.session;
				ctx.manager = c.manager;
			})
			.catch((reason) => {
				error = reason instanceof Error ? reason : new Error(String(reason));
			});

		return () => {
			cancelled = true;
			if (resolved) {
				resolved.shutdown();
			}
		};
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
