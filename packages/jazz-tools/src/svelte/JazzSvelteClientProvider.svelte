<!--
Makes a caller-owned Jazz client available to descendant Svelte components.
The caller remains responsible for shutting the client down.
-->
<script lang="ts">
	import { startInspectorOnce } from '../dev-tools/auto-attach.js';
	import type { Db } from '../runtime/db.js';
	import { withCanonicalUser } from '../runtime/author-id.js';
	import { getSubscriptionStore } from '../subscription-store-internal.js';
	import { initJazzContext } from './context.svelte.js';
	import type { JazzClient } from './create-jazz-client.js';

	interface Props {
		client: JazzClient | Promise<JazzClient>;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
		autoAttachDevTools?: boolean;
	}

	let { client, children, fallback, autoAttachDevTools = true }: Props = $props();

	const ctx = initJazzContext();
	let error = $state<Error | null>(null);

	function clearContext(): void {
		ctx.db = null;
		ctx.session = null;
		ctx.subscriptionStore = null;
	}

	$effect(() => {
		let cancelled = false;
		let stopSessionSync: (() => void) | null = null;
		const nextClient = client;

		error = null;
		clearContext();

		Promise.resolve(nextClient)
			.then((resolvedClient) => {
				if (cancelled) {
					return;
				}

				ctx.db = resolvedClient.db;
				ctx.session = resolvedClient.session ?? null;
				ctx.subscriptionStore = getSubscriptionStore(resolvedClient);
				stopSessionSync = resolvedClient.db.onAuthChanged(({ session }) => {
					if (!cancelled) {
						ctx.session = session ? withCanonicalUser(session) : null;
					}
				});

				if (process.env.NODE_ENV !== 'production' && autoAttachDevTools) {
					startInspectorOnce(resolvedClient.db);
				}
			})
			.catch((reason) => {
				if (!cancelled) {
					error = reason instanceof Error ? reason : new Error(String(reason));
				}
			});

		return () => {
			cancelled = true;
			stopSessionSync?.();
			clearContext();
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
