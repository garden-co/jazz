<!--
Makes a Jazz client available to descendant Svelte components through context.
Pass a reactive database configuration. The provider creates the client and
serialises shutdown before starting a replacement.
-->
<script lang="ts">
	import type { Db } from '../runtime/db.js';
	import type { AccountDbConfig as DbConfig } from '../accounts/context.js';
	import { serializeClientConfig } from '../runtime/client-config-key.js';
	import JazzSvelteClientProvider from './JazzSvelteClientProvider.svelte';
	import { createJazzClient, type JazzClient } from './create-jazz-client.js';
	import { assertNoClassicProviderProps } from '../classic-api.js';

	interface Props {
		config: DbConfig;
		children: import('svelte').Snippet<[{ db: Db }]>;
		fallback?: import('svelte').Snippet;
		autoAttachDevTools?: boolean;
	}

	type ConfigSnapshot =
		| { config: DbConfig; key: string }
		| { error: unknown };

	function isPlainRecord(value: object): value is Record<string, unknown> {
		const prototype = Object.getPrototypeOf(value);
		return prototype === Object.prototype || prototype === null;
	}

	function cloneConfigValue(value: unknown, seen: WeakMap<object, unknown>): unknown {
		if (value === null || typeof value !== 'object') {
			return value;
		}

		const previous = seen.get(value);
		if (previous !== undefined) {
			return previous;
		}

		if (value instanceof Date) {
			return new Date(value.getTime());
		}

		if (Array.isArray(value)) {
			const clone: unknown[] = [];
			seen.set(value, clone);
			for (let index = 0; index < value.length; index++) {
				clone[index] = cloneConfigValue(value[index], seen);
			}
			return clone;
		}

		if (!isPlainRecord(value)) {
			return value;
		}

		const clone = Object.create(Object.getPrototypeOf(value)) as Record<string, unknown>;
		seen.set(value, clone);
		for (const key of Object.keys(value)) {
			clone[key] = cloneConfigValue(value[key], seen);
		}
		return clone;
	}

	function captureConfig(config: DbConfig): DbConfig {
		// Config is a known in-process DbConfig; cloning only plain values gives
		// the async handover an immutable snapshot while retaining opaque refs.
		const captured = cloneConfigValue(config, new WeakMap()) as DbConfig;
		return captured;
	}

	function toError(reason: unknown): Error {
		return reason instanceof Error ? reason : new Error(String(reason));
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
	let previousSnapshot: ConfigSnapshot | undefined;

	let configSnapshot = $derived.by((): ConfigSnapshot => {
		try {
			const captured = captureConfig(config);
			const nextSnapshot = { config: captured, key: serializeClientConfig(captured) };
			if (
				previousSnapshot &&
				'config' in previousSnapshot &&
				previousSnapshot.key === nextSnapshot.key
			) {
				return previousSnapshot;
			}
			previousSnapshot = nextSnapshot;
			return nextSnapshot;
		} catch (reason) {
			return { error: reason };
		}
	});

	$effect(() => {
		validateClassicProps();
		let cancelled = false;
		const snapshot = configSnapshot;

		error = null;
		client = null;

		const clientToShutdown = () => {
			const current = activeClient;
			activeClient = null;
			const shuttingDown = handover.then(() => current?.shutdown());
			shuttingDown.catch(() => {});
			handover = shuttingDown;
		};

		if ('error' in snapshot) {
			clientToShutdown();
			error = toError(snapshot.error);
			return () => {
				cancelled = true;
				client = null;
			};
		}

		const nextConfig = snapshot.config;
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

				error = toError(reason);
			});

		return () => {
			cancelled = true;
			client = null;
			clientToShutdown();
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
