<script lang="ts">
	import { getDb } from 'jazz-tools/svelte';
	import { getCurrentJam, ensureInstrumentsSeeded } from './jam.js';
	import { getHashJamId, setHashJamId, onHashChange } from './router.svelte.js';
	import AudioProvider from './AudioProvider.svelte';
	import Nav from './Nav.svelte';
	import Sequencer from './Sequencer.svelte';
	import Participants from './Participants.svelte';
	import InstrumentManager from './InstrumentManager.svelte';
	import { getAudioContext } from './audio-context.js';

	const db = getDb();
	let jamId = $state<string | null>(null);
	let error = $state<string | null>(null);
	let initialised = false;

	async function resolveJam() {
		error = null;
		const hashId = getHashJamId();

		if (hashId) {
			// Trust the URL: use the jam ID directly.
			// Components subscribe to data reactively, so they will
			// render as data arrives via server sync.
			jamId = hashId;
		} else {
			// No jam in URL, create/find one for the current minute
			const id = await getCurrentJam(db);
			setHashJamId(id);
			jamId = id;
		}
	}

	$effect(() => {
		if (initialised) return;
		initialised = true;
		ensureInstrumentsSeeded(db).then(() => resolveJam());

		return onHashChange(() => {
			const hashId = getHashJamId();
			if (hashId && hashId !== jamId) {
				jamId = hashId;
			}
		});
	});
</script>

{#if jamId}
	<AudioProvider {jamId}>
		{@const audio = getAudioContext()}
		<Nav />
		<main>
			{#if audio.isContextActive}
				<Sequencer {jamId} />
				<div class="sidebar">
					<Participants {jamId} />
					<InstrumentManager />
				</div>
			{:else}
				<div class="start-prompt">
					<button onclick={() => audio.startContext()}>
						Start Wequencing
					</button>
				</div>
			{/if}
		</main>
	</AudioProvider>
{:else}
	<Nav />
	<main>
		{#if error}
			<div class="loading">{error}</div>
		{:else}
			<div class="loading">Loading...</div>
		{/if}
	</main>
{/if}
