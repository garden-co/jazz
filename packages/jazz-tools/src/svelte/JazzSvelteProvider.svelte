<!--
Makes a Jazz client available to descendant Svelte components through context.
Pass a pre-created client or a promise that resolves to one.

For SSR/hydration, set `ssr` (and pass `appId`) and leave `client` undefined until
the browser connects. The provider then renders straight away from a db-less
orchestrator that each `new QuerySubscription(query, { snapshot })` seeds, so the
server HTML and the first paint already hold the rows. When `client` resolves its
db attaches to that same orchestrator — the queued sync bundle fills the store and
every query re-subscribes — so there's no flash and live updates just start.
-->
<script lang="ts">
	import { onDestroy } from 'svelte';
	import type { Db } from '../runtime/db.js';
	import { createDbLessOrchestrator } from '../ssr/seed-orchestrator.js';
	import { initJazzContext } from './context.svelte.js';
	import type { JazzClient } from './create-jazz-client.js';

	interface Props {
		/** The live client, or a promise of one. Optional: in the SSR seed phase it
		 * is absent until the browser connects, and the seeded rows render until then. */
		client?: JazzClient | Promise<JazzClient>;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
		/** Opt into the synchronous SSR seed phase (see the component comment). */
		ssr?: boolean;
		/** App id for the seed orchestrator's query keys; only read in the seed phase. */
		appId?: string;
	}

	let { client, children, fallback, ssr, appId }: Props = $props();

	const ctx = initJazzContext();
	let error = $state<Error | null>(null);

	// One orchestrator for the whole seed→live transition. Created synchronously so
	// it's present during SSR (where $effect never runs) and on the client's first
	// render, before any child QuerySubscription seeds it. The live db attaches to
	// this same instance once the client connects — no swap, no flash. It stays
	// null when `ssr` is off: the normal path adopts the client's own orchestrator.
	//
	// The `ssr`/`appId` props are read inside this closure, not at the top-level
	// instance scope, so the compiler doesn't flag them as `state_referenced_locally`
	// — they're read once at init by design, never reactively.
	function buildSeedManager() {
		return ssr ? createDbLessOrchestrator(appId ?? '') : null;
	}

	const seedManager = buildSeedManager();
	if (seedManager) {
		ctx.manager = seedManager;
	}

	$effect(() => {
		let cancelled = false;
		let resolvedClient: JazzClient | null = null;
		let stopSessionSync: (() => void) | null = null;

		error = null;
		ctx.db = null;
		ctx.session = null;
		// Keep the seed orchestrator on screen (not null) so seeded rows stay until
		// the live client connects. In the normal path this is null, as before.
		ctx.manager = seedManager;

		// No client yet (SSR, or before the browser connects): render from the seed
		// orchestrator alone until one is supplied.
		if (!client) {
			return;
		}

		Promise.resolve(client)
			.then((resolved) => {
				if (cancelled) {
					resolved.shutdown().catch(() => {});
					return;
				}

				resolvedClient = resolved;

				if (seedManager) {
					// SSR: attach the live db to the orchestrator the hooks seeded. One
					// pass drains the queued bundle and re-subscribes every query.
					seedManager.attachDb(resolved.db, resolved.session ?? null);
					ctx.db = resolved.db;
					ctx.session = resolved.session ?? null;
					stopSessionSync = resolved.db.onAuthChanged(({ session }) => {
						if (cancelled) return;
						ctx.session = session ?? null;
						seedManager.setSession(session ?? null);
					});
				} else {
					// Normal path: adopt the client's own orchestrator.
					ctx.db = resolved.db;
					ctx.session = resolved.session ?? null;
					ctx.manager = resolved.manager;
					stopSessionSync = resolved.db.onAuthChanged(({ session }) => {
						if (cancelled) return;
						ctx.session = session ?? null;
					});
				}
			})
			.catch((reason) => {
				if (cancelled) return;
				error = reason instanceof Error ? reason : new Error(String(reason));
			});

		return () => {
			cancelled = true;
			stopSessionSync?.();
			if (resolvedClient) {
				resolvedClient.shutdown().catch(() => {});
			}
		};
	});

	onDestroy(() => {
		seedManager?.shutdown().catch(() => {});
	});
</script>

{#if error}
	<!-- Re-throw so an error boundary can catch it -->
	{(() => {
		throw error;
	})()}
{:else if ctx.db || ctx.manager}
	<!-- In the seed phase ctx.db is still null; seed-phase children read rows via
	     QuerySubscription and the db via getDb() once it's ready, not this param. -->
	{@render children({ db: ctx.db as Db })}
{:else if fallback}
	{@render fallback()}
{/if}
