<script lang="ts">
	import { getDb } from 'jazz-tools/svelte';
	import { app } from '../schema/app.js';
	import { getAudioContext } from './audio-context.js';
	import { setHashJamId } from './router.svelte.js';

	const db = getDb();

	let audio: ReturnType<typeof getAudioContext> | null = $state(null);
	try {
		audio = getAudioContext();
	} catch {
		audio = null;
	}

	let now = $state(Date.now());
	$effect(() => {
		let timeout: ReturnType<typeof setTimeout> | null = null;
		if (audio && audio.countdownMs > 0) {
			const tick = () => {
				now = Date.now();
				if (audio!.countdownMs - now > 0) {
					timeout = setTimeout(tick, 200);
				}
			};
			tick();
		}
		return () => {
			if (timeout) clearTimeout(timeout);
		};
	});

	let copied = $state(false);

	async function share() {
		const url = window.location.href;
		if (navigator.share) {
			try {
				await navigator.share({ title: 'Wequencer', url });
				return;
			} catch {
				// User cancelled or share failed, fall through to clipboard
			}
		}
		await navigator.clipboard.writeText(url);
		copied = true;
		setTimeout(() => (copied = false), 2000);
	}

	// History lookup
	let historyOpen = $state(false);
	let historyValue = $state('');
	let historyError = $state('');

	async function lookupJam() {
		historyError = '';
		if (!historyValue) return;

		const date = new Date(historyValue);
		if (isNaN(date.getTime())) {
			historyError = 'Invalid date';
			return;
		}

		// Floor to the minute
		const epochMs = Math.floor(date.getTime() / 60_000) * 60_000;
		const floored = new Date(epochMs);

		const results = await db.all(app.jams.where({ created_at: floored }).limit(1), 'worker');
		if (results.length > 0) {
			setHashJamId(results[0].id);
			historyOpen = false;
			historyValue = '';
		} else {
			historyError = 'No jam found at that time';
		}
	}
</script>

<nav>
	<div class="nav-left">
		<h1>Wequencer</h1>
		<button class="share-btn" onclick={share}>
			{copied ? 'Copied!' : 'Share'}
		</button>
		<button class="share-btn" onclick={() => (historyOpen = !historyOpen)}>
			History
		</button>
	</div>

	{#if historyOpen}
		<div class="history-popover">
			<input
				class="history-input"
				type="datetime-local"
				bind:value={historyValue}
			/>
			<button class="history-go" onclick={lookupJam}>Go</button>
			{#if historyError}
				<span class="history-error">{historyError}</span>
			{/if}
		</div>
	{/if}

	{#if audio}
		<div class="transport">
			<div class="bpm-control">
				<button class="bpm-btn" onclick={() => audio!.setBpm(audio!.bpm - 5)}>-</button>
				<span class="bpm-value">{audio.bpm}</span>
				<span class="bpm-label">BPM</span>
				<button class="bpm-btn" onclick={() => audio!.setBpm(audio!.bpm + 5)}>+</button>
			</div>

			<div class="bpm-control">
				<button class="bpm-btn" onclick={() => audio!.setBeatCount(audio!.beatCount - 4)}>-</button>
				<span class="bpm-value">{audio.beatCount / 4}</span>
				<span class="bpm-label">Bars</span>
				<button class="bpm-btn" onclick={() => audio!.setBeatCount(audio!.beatCount + 4)}>+</button>
			</div>

			<div class="display">
				<span class="bar-label">Bar</span>
				<span class="bar-value">
					{#if audio.countdownMs - now > 0}
						-{Math.ceil((audio.countdownMs - now) / (60_000 / audio.bpm))}
					{:else}
						{Math.floor(audio.uiBeat / 4 + 1)}
					{/if}
				</span>
				<span class="beat-label">Beat</span>
				<span class="beat-value">
					{#if audio.countdownMs - now > 0}
						{Math.min(Math.ceil((audio.countdownMs - now) / (60_000 / audio.bpm)), 4)}
					{:else}
						{(audio.uiBeat % 4) + 1}
					{/if}
				</span>
			</div>

			{#if audio.isContextActive}
				{#if audio.isPlaying}
					<button class="stop-btn" onclick={() => audio!.stop()}>Stop</button>
				{:else}
					<button class="play-btn" onclick={() => audio!.play()}>Play</button>
				{/if}
			{/if}
		</div>
	{/if}
</nav>
