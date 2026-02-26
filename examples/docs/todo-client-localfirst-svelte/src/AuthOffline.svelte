<!-- #region auth-offline-svelte -->
<script lang="ts">
	import { JazzSvelteProvider } from 'jazz-tools/svelte';
</script>

<JazzSvelteProvider config={{ appId: 'my-app' }}>
	{#snippet children({ db })}
		<slot />
	{/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-offline-svelte -->
