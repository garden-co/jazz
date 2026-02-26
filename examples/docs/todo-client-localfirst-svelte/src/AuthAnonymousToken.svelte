<!-- #region auth-anon-token-svelte -->
<script lang="ts">
	import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

	const client = createJazzClient({
		appId: 'my-app',
		localAuthMode: 'anonymous',
		localAuthToken: 'device-token-123',
	});
</script>

<JazzSvelteProvider {client}>
	{#snippet children({ db })}
		<slot />
	{/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-anon-token-svelte -->
