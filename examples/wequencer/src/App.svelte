<script lang="ts">
	import { createJazzClient, JazzSvelteProvider, BrowserAuthSecretStore } from 'jazz-tools/svelte';
	import Main from './Main.svelte';

	const appId = import.meta.env.VITE_JAZZ_APP_ID;
	const serverUrl = import.meta.env.DEV
		? window.location.origin
		: import.meta.env.VITE_JAZZ_SERVER_URL;

	const client = BrowserAuthSecretStore.getOrCreateSecret().then((secret) =>
		createJazzClient({ appId, serverUrl, secret }),
	);
</script>

<JazzSvelteProvider {client}>
	{#snippet children({ db })}
		<Main />
	{/snippet}
</JazzSvelteProvider>
