<!-- #region auth-anon-token-svelte -->
<script lang="ts">
	import { JazzSvelteProvider } from 'jazz-tools/svelte';
</script>

<JazzSvelteProvider
	config={{
		appId: 'my-app',
		localAuthMode: 'anonymous',
		localAuthToken: 'device-token-123',
	}}
>
	{#snippet children({ db })}
		<slot />
	{/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-anon-token-svelte -->
