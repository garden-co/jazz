<!--
Makes a Jazz client available to descendant Svelte components through context.
Pass a reactive database configuration. The provider creates the client and
serialises shutdown before starting a replacement.
-->
<script lang="ts">
	import type { Db } from '../runtime/db.js';
	import type { AccountDbConfig as DbConfig } from '../accounts/context.js';
	import JazzSvelteClientProvider from './JazzSvelteClientProvider.svelte';
	import { createJazzClient, type JazzClient } from './create-jazz-client.js';
	import { assertNoClassicProviderProps } from '../classic-api.js';

	interface Props {
		config: DbConfig;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
		autoAttachDevTools?: boolean;
	}

	let { config, children, fallback, autoAttachDevTools = true, ...otherProps }: Props = $props();

	function validateClassicProps() {
		assertNoClassicProviderProps('JazzSvelteProvider', otherProps);
	}

	validateClassicProps();

	let error = $state<Error | null>(null);
	let client = $state<JazzClient | null>(null);
	let activeClient: JazzClient | null = null;
	let handover = Promise.resolve();

	$effect(() => {
		validateClassicProps();
		let cancelled = false;
		const nextConfig = config;

		error = null;
		client = null;

		handover = handover
			.then(async () => {
				if (cancelled) {
					return;
				}

				validateClassicProps();
				const createdClient = await createJazzClient(nextConfig);
				if (cancelled) {
					await createdClient.shutdown();
					return;
				}
				try {
					validateClassicProps();
				} catch (reason) {
					await createdClient.shutdown();
					throw reason;
				}

				activeClient = createdClient;
				client = createdClient;
			})
			.catch((reason) => {
				if (cancelled) {
					return;
				}

				error = reason instanceof Error ? reason : new Error(String(reason));
			});

		return () => {
			cancelled = true;
			client = null;
			const clientToShutdown = activeClient;
			activeClient = null;
			const shuttingDown = handover.then(() => clientToShutdown?.shutdown());
			shuttingDown.catch(() => {});
			handover = shuttingDown;
		};
	});
</script>

{#if error}
	<!-- Re-throw so an error boundary can catch it -->
	{(() => { throw error; })()}
{:else if client}
	<JazzSvelteClientProvider {client} {fallback} {autoAttachDevTools}>
		{#snippet children({ db })}
			{@render children({ db })}
		{/snippet}
	</JazzSvelteClientProvider>
{:else if fallback}
	{@render fallback()}
{/if}
