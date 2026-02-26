<!-- #region auth-external-svelte -->
<script lang="ts">
	import { createJazzClient, JazzSvelteProvider, useLinkExternalIdentity } from 'jazz-tools/svelte';

	const appId = 'my-app';
	const serverUrl = 'http://127.0.0.1:4200';

	const linkExternalIdentity = useLinkExternalIdentity({
		appId,
		serverUrl,
		defaultMode: 'anonymous',
	});

	let jwtToken = $state<string | undefined>();

	async function onSignedIn(providerJwt: string) {
		await linkExternalIdentity({ jwtToken: providerJwt });
		jwtToken = providerJwt;
	}

	const client = $derived(
		createJazzClient({
			appId,
			serverUrl,
			jwtToken,
		}),
	);
</script>

{#key jwtToken}
	<JazzSvelteProvider {client}>
		{#snippet children({ db })}
			<button onclick={() => onSignedIn('<provider-jwt>')}>Sign in</button>
			<slot />
		{/snippet}
	</JazzSvelteProvider>
{/key}
<!-- #endregion auth-external-svelte -->
