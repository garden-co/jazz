<!-- #region auth-anon-svelte -->
<script lang="ts">
	import { JazzSvelteProvider } from 'jazz-tools/svelte';
</script>

<JazzSvelteProvider
	config={{
		appId: 'my-app',
		serverUrl: 'http://127.0.0.1:4200',
	}}
>
	{#snippet children({ db })}
		<slot />
	{/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-anon-svelte -->
