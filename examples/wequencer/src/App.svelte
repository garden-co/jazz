<script lang="ts">
	import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
	import Main from './Main.svelte';

	const appId = import.meta.env.VITE_JAZZ_APP_ID ?? 'wequencer';
	const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

	const client = createJazzClient({
		appId,
		serverUrl,
		env: 'dev',
		userBranch: 'main',
	});
</script>

<JazzSvelteProvider {client}>
	{#snippet children({ db })}
		<Main />
	{/snippet}
</JazzSvelteProvider>
