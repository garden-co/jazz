<!--
Makes a caller-owned Jazz client available to descendant Svelte components.
The caller remains responsible for shutting the client down.
-->
<script lang="ts">
	import type { Db } from '../runtime/db.js';
	import { getSubscriptionStore } from '../subscription-store-internal.js';
	import { initJazzContext } from './context.svelte.js';
	import { untrack } from 'svelte';
	import { hideJazzSession } from './session-state.js';
	import type { JazzClient } from './create-jazz-client.js';
	import { toError } from '../runtime/util.js';

	interface Props {
		client: JazzClient | Promise<JazzClient>;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
		/** @internal Set by `<JazzProvider>` for its session's own client. */
		inSession?: boolean;
	}

	let { client, children, fallback, inSession = false }: Props = $props();

	// Its children read this client, not an enclosing <JazzProvider>'s session.
	// Context is set once, at init, so the initial value is the one that counts.
	if (!untrack(() => inSession)) hideJazzSession();

	const ctx = initJazzContext();
	let error = $state<Error | null>(null);

	function clearContext(): void {
		ctx.db = null;
		ctx.session = null;
		ctx.client = null;
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
				ctx.client = resolvedClient;
				ctx.subscriptionStore = getSubscriptionStore(resolvedClient);
				stopSessionSync = resolvedClient.db.onAuthChanged(({ session }) => {
					if (!cancelled) {
						ctx.session = session;
					}
				});
			})
			.catch((reason) => {
				if (!cancelled) {
					error = toError(reason);
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
