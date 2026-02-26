<script lang="ts">
	import { onDestroy } from 'svelte';
	import { createDb, type DbConfig } from '../runtime/db.js';
	import type { Db } from '../runtime/db.js';
	import { resolveLocalAuthDefaults } from '../runtime/local-auth.js';
	import { resolveClientSession } from '../runtime/client-session.js';
	import { initJazzContext } from './context.svelte.js';

	interface Props {
		config: DbConfig;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
	}

	let { config, children, fallback }: Props = $props();

	const ctx = initJazzContext();
	let instance: Db | null = null;
	let error = $state<Error | null>(null);
	let cancelled = false;

	const resolvedConfig = resolveLocalAuthDefaults(config);

	Promise.all([createDb(resolvedConfig), resolveClientSession(resolvedConfig)])
		.then(([created, session]) => {
			if (cancelled) {
				void created.shutdown();
				return;
			}
			instance = created;
			ctx.db = created;
			ctx.session = session;
		})
		.catch((reason) => {
			error = reason instanceof Error ? reason : new Error(String(reason));
		});

	onDestroy(() => {
		cancelled = true;
		if (instance) {
			void instance.shutdown();
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
