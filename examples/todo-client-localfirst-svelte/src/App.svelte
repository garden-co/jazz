<script lang="ts">
	import {
		createJazzClient,
		JazzSvelteProvider,
	} from 'jazz-tools/svelte';
	import type { DbConfig } from 'jazz-tools';
	import { BrowserAuthSecretStore, generateAuthSecret } from 'jazz-tools';
	import { Toaster } from 'svelte-sonner';
	import TodoList from './TodoList.svelte';

	interface Props {
		config?: Partial<DbConfig>;
	}

	let { config: configOverrides = {} }: Props = $props();

	function readEnvAppId(): string | undefined {
		return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
			?.JAZZ_APP_ID;
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
		const appId = overrides.appId ?? readEnvAppId() ?? '019d4349-2408-7275-9b65-ac87f62b7aa2';
		const secret = overrides.auth?.localFirstSecret ?? getOrCreateSecretSync();

		return {
			appId,
			env: 'dev',
			userBranch: 'main',
			auth: { localFirstSecret: secret },
			...overrides,
		};
	}
	// #endregion context-setup-svelte

	const config = defaultConfig(configOverrides);
	const client = createJazzClient(config);
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
