<script lang="ts">
	import {
		createJazzClient,
		JazzSvelteProvider,
		SyntheticUserSwitcher,
		getActiveSyntheticAuth,
	} from 'jazz-tools/svelte';
	import type { DbConfig } from 'jazz-tools';
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
		const appId = overrides.appId ?? readEnvAppId() ?? 'todo-svelte-example';
		const active = getActiveSyntheticAuth(appId, { defaultMode: 'demo' });

		return {
			appId,
			env: 'dev',
			userBranch: 'main',
			localAuthMode: active.localAuthMode,
			localAuthToken: active.localAuthToken,
			...overrides,
		};
	}
	// #endregion context-setup-svelte

	const config = defaultConfig(configOverrides);
	const client = createJazzClient(config);
</script>

<SyntheticUserSwitcher appId={config.appId} defaultMode="demo" />
<JazzSvelteProvider {client}>
	{#snippet children({ db })}
		<h1>Todos</h1>
		<TodoList />
	{/snippet}
	{#snippet fallback()}
		<p>Loading...</p>
	{/snippet}
</JazzSvelteProvider>
