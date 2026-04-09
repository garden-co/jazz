<script lang="ts">
	import {
		createJazzClient,
		JazzSvelteProvider,
	} from 'jazz-tools/svelte';
	import { loadOrCreateIdentitySeed, mintSelfSignedToken, type DbConfig } from 'jazz-tools';
	import TodoList from './TodoList.svelte';

	interface Props {
		config?: Partial<DbConfig>;
	}

	let { config: configOverrides = {} }: Props = $props();

	function readEnvAppId(): string | undefined {
		return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
			?.JAZZ_APP_ID;
	}

	// #region context-setup-svelte
	function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
		const appId = overrides.appId ?? readEnvAppId() ?? '019d4349-2408-7275-9b65-ac87f62b7aa2';
		const seed = loadOrCreateIdentitySeed(appId);
		const jwtToken = mintSelfSignedToken(seed.seed, appId);

		return {
			appId,
			env: 'dev',
			userBranch: 'main',
			jwtToken,
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
	{/snippet}
	{#snippet fallback()}
		<p>Loading...</p>
	{/snippet}
</JazzSvelteProvider>
