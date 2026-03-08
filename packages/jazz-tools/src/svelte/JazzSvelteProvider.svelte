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
	let resolvedClient = $state<JazzClient | null>(null);
	let error = $state<Error | null>(null);
	let cancelled = false;

	$effect(() => {
		let active = true;

		Promise.resolve(client)
			.then((c) => {
				if (cancelled || !active) {
					void c.shutdown();
					return;
				}
				// Publish session before db so child components never observe a ready db
				// with a stale null session during the first render tick.
				ctx.session = c.session;
				ctx.db = c.db;
				resolvedClient = c;
			})
			.catch((reason) => {
				if (!active) {
					return;
				}
				error = reason instanceof Error ? reason : new Error(String(reason));
			});

		return () => {
			active = false;
		};
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
{:else if resolvedClient}
	{@render children({ db: resolvedClient.db })}
{:else if fallback}
	{@render fallback()}
{/if}
