<!-- #region auth-demo-svelte -->
<script lang="ts">
	import {
		JazzSvelteProvider,
		SyntheticUserSwitcher,
		getActiveSyntheticAuth,
	} from 'jazz-tools/svelte';

	const appId = 'my-app';
	const active = getActiveSyntheticAuth(appId, { defaultMode: 'demo' });

	const config = {
		appId,
		serverUrl: 'http://127.0.0.1:4200',
		localAuthMode: active.localAuthMode,
		localAuthToken: active.localAuthToken,
	};
</script>

<SyntheticUserSwitcher {appId} defaultMode="demo" />
<JazzSvelteProvider {config}>
	{#snippet children({ db })}
		<slot />
	{/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-demo-svelte -->
