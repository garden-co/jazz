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
		let resolvedClient: JazzClient | null = null;
		let stopSessionSync: (() => void) | null = null;

		error = null;
		ctx.db = null;
		ctx.session = null;
		ctx.manager = null;

		Promise.resolve(client)
			.then((resolved) => {
				if (cancelled) {
					void resolved.shutdown();
					return;
				}

				resolvedClient = resolved;
				ctx.db = resolved.db;
				ctx.session = resolved.session ?? null;
				ctx.manager = resolved.manager;
				stopSessionSync = resolved.db.onAuthChanged(({ session }) => {
					if (cancelled) {
						return;
					}

					ctx.session = session ?? null;
				});
			})
			.catch((reason) => {
				if (cancelled) {
					return;
				}

				error = reason instanceof Error ? reason : new Error(String(reason));
			});

		return () => {
			cancelled = true;
			stopSessionSync?.();
			if (resolvedClient) {
				void resolvedClient.shutdown();
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
