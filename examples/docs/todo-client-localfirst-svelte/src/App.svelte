<!-- #region provider-svelte -->
<script lang="ts">
	import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
	import TodoList from './TodoList.svelte';

	const client = createJazzClient({
		appId: 'todo-svelte-example',
		env: 'dev',
		userBranch: 'main',
	});
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
<!-- #endregion provider-svelte -->
