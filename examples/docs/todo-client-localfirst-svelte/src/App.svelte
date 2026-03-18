<!-- #region provider-svelte -->
<script lang="ts">
	import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
	import AuthSessionExamples from './AuthSessionExamples.svelte';
	import TodoList from './TodoList.svelte';

	// Keep docs-only auth snippets in the compiled example app.
	void AuthSessionExamples;

	const client = createJazzClient({
		appId: 'todo-svelte-example',
		serverUrl: 'http://127.0.0.1:1625',
		localAuthMode: 'anonymous',
		// jwtToken: authToken, // Use this (instead of localAuthMode) for external auth.
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
