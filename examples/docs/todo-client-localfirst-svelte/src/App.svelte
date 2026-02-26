<!-- #region provider-svelte -->
<script lang="ts">
	import { JazzSvelteProvider } from 'jazz-tools/svelte';
	import type { DbConfig } from 'jazz-tools';
	import TodoList from './TodoList.svelte';

	const config: DbConfig = {
		appId: 'todo-svelte-example',
		env: 'dev',
		userBranch: 'main',
	};
</script>

<JazzSvelteProvider {config}>
	{#snippet children({ db })}
		<h1>Todos</h1>
		<TodoList />
	{/snippet}
	{#snippet fallback()}
		<p>Loading...</p>
	{/snippet}
</JazzSvelteProvider>
<!-- #endregion provider-svelte -->
