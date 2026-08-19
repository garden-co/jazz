<!--
Makes a Jazz client available to descendant Svelte components through context.
Pass a reactive database configuration. The provider creates the client and
serialises shutdown before starting a replacement.
-->
<script lang="ts">
	import type { Db, DbConfig } from '../runtime/db.js';
	import { getSubscriptionStore } from '../subscription-store-internal.js';
	import { initJazzContext } from './context.svelte.js';
	import { createJazzClient, type JazzClient } from './create-jazz-client.js';
	import { startInspectorOnce } from '../dev-tools/auto-attach.js';

	interface Props {
		config: DbConfig;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
		autoAttachDevTools?: boolean;
	}

	let { config, children, fallback, autoAttachDevTools = true }: Props = $props();

	const ctx = initJazzContext();
	let error = $state<Error | null>(null);
	let activeClient: JazzClient | null = null;
	let stopSessionSync: (() => void) | null = null;
	let handover = Promise.resolve();

	function clearContext(): void {
		ctx.db = null;
		ctx.session = null;
		ctx.subscriptionStore = null;
	}

	$effect(() => {
		let cancelled = false;
		const nextConfig = config;

		error = null;
		clearContext();

		handover = handover
			.then(async () => {
				if (cancelled) {
					return;
				}

				const client = await createJazzClient(nextConfig);
				if (cancelled) {
					await client.shutdown();
					return;
				}

				activeClient = client;
				ctx.db = client.db;
				ctx.session = client.session ?? null;
				ctx.subscriptionStore = getSubscriptionStore(client);
				stopSessionSync = client.db.onAuthChanged(({ session }) => {
					if (cancelled) {
						return;
					}

					ctx.session = session ?? null;
				});

				if (process.env.NODE_ENV !== 'production' && autoAttachDevTools) {
					startInspectorOnce(client.db);
				}
			})
			.catch((reason) => {
				if (cancelled) {
					return;
				}

				error = reason instanceof Error ? reason : new Error(String(reason));
			});

		return () => {
			cancelled = true;
			clearContext();
			stopSessionSync?.();
			stopSessionSync = null;
			const client = activeClient;
			activeClient = null;
			const shuttingDown = handover.then(() => client?.shutdown());
			shuttingDown.catch(() => {});
			handover = shuttingDown;
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
