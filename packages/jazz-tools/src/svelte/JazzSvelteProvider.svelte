<script lang="ts">
	import { onDestroy } from 'svelte';
	import type { DehydratedSnapshot } from '../backend/ssr.js';
	import {
		computeSchemaFingerprint,
		resolveWasmSchema,
		type WasmSchemaInput
	} from '../drivers/schema-wire.js';
	import { applySnapshot } from '../ssr/apply-snapshot.js';
	import { createSeedOrchestrator } from '../ssr/seed-orchestrator.js';
	import { initJazzContext } from './context.svelte.js';
	import type { JazzClient } from './create-jazz-client.js';

	interface Props {
		/** The live client, or a promise of one. Optional: with a `snapshot`, the
		 * provider renders the seeded rows until a client is supplied (e.g. during
		 * SSR, or before the browser connects). */
		client?: JazzClient | Promise<JazzClient>;
		children: import('svelte').Snippet;
		fallback?: import('svelte').Snippet;
		/** Server-rendered query results to seed the first paint with. */
		snapshot?: DehydratedSnapshot;
		/** Client schema; when set, the snapshot's fingerprint is checked against it. */
		schema?: WasmSchemaInput;
		/** Overrides the appId the snapshot is validated against (defaults to the snapshot's). */
		expectedAppId?: string;
	}

	let { client, children, fallback, snapshot, schema, expectedAppId }: Props = $props();

	const ctx = initJazzContext();
	let error = $state<Error | null>(null);

	// Prop reads live inside these helpers (closures), not the top-level instance
	// scope, so the compiler doesn't flag them as `state_referenced_locally` —
	// the seed/snapshot props are read once at init by design, never reactively.
	function snapshotFingerprint(): string | undefined {
		return schema ? computeSchemaFingerprint(resolveWasmSchema(schema)) : undefined;
	}

	function snapshotExpected(principalId: string | null) {
		return {
			appId: expectedAppId ?? snapshot!.appId,
			principalId,
			schemaFingerprint: snapshotFingerprint() ?? snapshot!.schemaFingerprint
		};
	}

	// Synchronous seed: runs on the server (where $effect never fires) and on
	// the client's first init, so seeded rows are present before any effect.
	function buildSeedManager() {
		return snapshot ? createSeedOrchestrator(snapshot, snapshotExpected(null)) : null;
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
		// Fall back to the seed manager (not null) so seeded rows stay visible
		// until the live client connects — no empty flash after hydration.
		ctx.manager = seedManager;

		// No live client yet (during SSR, or before the browser connects): render
		// from the seed alone until one is supplied.
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
				if (snapshot) {
					// Re-seed the live orchestrator with the same rows, now checked
					// against the live principal, before swapping it in.
					applySnapshot({
						manager: resolved.manager,
						snapshot,
						expected: snapshotExpected(resolved.session?.user_id ?? null)
					});
				}
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
	{(() => { throw error; })()}
{:else if ctx.db || ctx.manager}
	{@render children()}
{:else if fallback}
	{@render fallback()}
{/if}
