<script lang="ts">
	import {
		createJazzClient,
		JazzSvelteProvider,
	} from 'jazz-tools/svelte';
	import type { DbConfig } from 'jazz-tools';
	import { generateAuthSecret } from 'jazz-tools';
	import { Toaster } from 'svelte-sonner';
	import TodoList from './TodoList.svelte';

	interface Props {
		config?: Partial<DbConfig>;
	}

	let { config: configOverrides = {} }: Props = $props();

	function readEnv(name: string): string | undefined {
		return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env?.[name];
	}

	function getOrCreateSecretSync(): string {
		const stored = localStorage.getItem('jazz-auth-secret');
		if (stored) return stored;
		const secret = generateAuthSecret();
		localStorage.setItem('jazz-auth-secret', secret);
		return secret;
	}

	// #region context-setup-svelte
	function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
		const appId = overrides.appId ?? readEnv('PUBLIC_JAZZ_APP_ID');
		const serverUrl = overrides.serverUrl ?? readEnv('PUBLIC_JAZZ_SERVER_URL');
		if (!appId)
			throw new Error('Missing appId: add jazzSvelteKit() to vite.config.ts or set PUBLIC_JAZZ_APP_ID');
		const secret = overrides.auth?.localFirstSecret ?? getOrCreateSecretSync();

		return {
			appId,
			env: 'dev',
			userBranch: 'main',
			auth: { localFirstSecret: secret },
			...(serverUrl ? { serverUrl } : {}),
			...overrides,
		};
	}
	// #endregion context-setup-svelte

	const config = $derived(defaultConfig(configOverrides));
	const client = $derived(createJazzClient(config));
</script>

<JazzSvelteProvider {client}>
	{#snippet children({ db })}
		<h1>Todos</h1>
		<TodoList />
		<Toaster />
	{/snippet}
	{#snippet fallback()}
		<p>Loading...</p>
	{/snippet}
</JazzSvelteProvider>
