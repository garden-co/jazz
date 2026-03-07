<script lang="ts">
	import { onDestroy, untrack } from 'svelte';
	import { SvelteMap } from 'svelte/reactivity';
	import { getDb, getSession, QuerySubscription } from 'jazz-tools/svelte';
	import {
		Player,
		Loop,
		getTransport,
		getDestination,
		now as toneNow,
		start,
		getDraw,
		Synth,
		Time,
		getContext,
		Gain,
		Panner,
	} from 'tone';

	import { app } from '../schema/app.js';
	import { setAudioContext } from './audio-context.js';
	import { ClockSync } from './clock-sync.svelte.js';
	import { DEFAULT_BEAT_COUNT, getRandomName } from './constants.js';

	// Types
	type LoadedPlayer = {
		instrumentId: string;
		player: Player | undefined;
	};

	type ChannelStrip = {
		gain: Gain;
		panner: Panner;
	};

	// Props
	let { jamId, children }: { jamId: string; children?: import('svelte').Snippet } = $props();

	// Jazz
	const db = getDb();
	const session = getSession();

	// Subscriptions — unfiltered to enable cross-user sync, filtered client-side
	const allJams = new QuerySubscription(app.jams);
	const jam = $derived({ current: (allJams.current ?? []).filter((j) => j.id === jamId) });
	const allBeats = new QuerySubscription(app.beats);
	const beats = $derived({ current: (allBeats.current ?? []).filter((b) => b.jam === jamId) });
	const instruments = new QuerySubscription(app.instruments.orderBy('display_order'));
	const allParticipants = new QuerySubscription(app.participants);
	const participants = $derived({
		current: (allParticipants.current ?? []).filter((p) => p.jam === jamId),
	});

	// Constants
	// Beat count is reactive, read from jam row
	let beatCount = $state(DEFAULT_BEAT_COUNT);
	let beatsArray = $derived(Array.from({ length: beatCount }, (_, i) => i));
	const DRIFT_HYSTERESIS_COUNT = 2;
	const DRIFT_THRESHOLD_MS = 10;
	const DRIFT_CHECK_INTERVAL_MS = 500;
	const MIN_SCHEDULING_BUFFER_MS = 100;
	const DEFAULT_BPM = 95;

	// Global instances
	const syncServerUrl = import.meta.env.VITE_JAZZ_SYNC_WS ?? 'wss://cloud.jazz.tools';
	const clockSync = new ClockSync(syncServerUrl);
	const transport = getTransport();

	// State
	const channelStrips = new Map<string, ChannelStrip>();
	let masterVolume = $state(0); // dB, 0 = unity
	let loadedPlayers = new SvelteMap<string, LoadedPlayer>();
	let currBeat = $state(0);
	let uiBeat = $state(0);
	let easterEggCounter = $state(0);
	let isPlaying = $state(false);
	let isStarting = $state(false);
	let isContextActive = $state(false);
	let transportStartTone: number | null = $state(null);
	let countdownMs = $state(0);
	let bpm = $state(DEFAULT_BPM);
	let baseBpm = DEFAULT_BPM;
	let syncAlignment = $state(localStorage.getItem('wequencer-sync-alignment') !== 'false');

	function setSyncAlignment(enabled: boolean) {
		syncAlignment = enabled;
		localStorage.setItem('wequencer-sync-alignment', String(enabled));
	}

	function setBpm(newBpm: number) {
		const clamped = Math.max(40, Math.min(300, Math.round(newBpm)));
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;
		db.update(app.jams, jamRows[0].id, { bpm: clamped });
	}

	function setBeatCount(count: number) {
		const rounded = Math.round(count / 4) * 4;
		const clamped = Math.max(4, Math.min(64, rounded));
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;
		db.update(app.jams, jamRows[0].id, { beat_count: clamped });
	}

	// Sync tracking
	let wasPlaying = false;
	let lastProcessedTransportStart: number | null = null;
	let isSyncing = false;
	let driftCount = 0;

	// Initialise transport
	transport.bpm.value = DEFAULT_BPM;

	// ====================
	// Audio Loop
	// ====================

	const loop = new Loop((time) => {
		const currentBeats = beats.current ?? [];
		loadedPlayers.forEach(({ instrumentId, player }) => {
			if (
				player &&
				currentBeats.some((b) => b.instrument === instrumentId && b.beat_index === currBeat)
			) {
				player.start(time);
			}
		});

		getDraw().schedule(() => {
			uiBeat = currBeat;
		}, time);

		currBeat = (currBeat + 1) % beatCount;
	}, '8n');

	// ====================
	// Player Management
	// ====================

	function getChannelStrip(instrumentId: string): ChannelStrip {
		let strip = channelStrips.get(instrumentId);
		if (!strip) {
			const gain = new Gain(1);
			const panner = new Panner(0);
			gain.connect(panner);
			panner.toDestination();
			strip = { gain, panner };
			channelStrips.set(instrumentId, strip);
		}
		return strip;
	}

	function loadPlayer(url: string, instrumentId: string): Promise<Player> {
		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => reject(new Error('Player load timeout')), 15000);

			try {
				const strip = getChannelStrip(instrumentId);
				const player = new Player(url, () => {
					clearTimeout(timeout);
					resolve(player);
				}).connect(strip.gain);
			} catch (err) {
				clearTimeout(timeout);
				reject(err);
			}
		});
	}

	function cleanUpPlayers() {
		loadedPlayers.forEach(({ player }) => {
			player?.dispose();
		});
		channelStrips.forEach(({ gain, panner }) => {
			gain.dispose();
			panner.dispose();
		});
		channelStrips.clear();
	}

	// ====================
	// Playback Control
	// ====================

	async function initiatePlay() {
		if (isStarting) return;
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;
		isStarting = true;

		try {
			if (!isContextActive) await startContext();
			if (loadedPlayers.size === 0) return;

			const isSolo = (participants.current ?? []).length <= 1;
			const skipSync = isSolo || !syncAlignment;
			let serverTargetEpoch: number;

			if (skipSync) {
				const nowEpoch = performance.timeOrigin + performance.now();
				serverTargetEpoch = clockSync.localToServer(nowEpoch) + MIN_SCHEDULING_BUFFER_MS;
			} else {
				const msPerBeat = ((60 / transport.bpm.value) * 1000) / 2;
				const msPerLoop = msPerBeat * beatCount;

				if (clockSync.lastHeartbeat != null) {
					const targetTime = clockSync.lastHeartbeat + 2000;
					const timeSinceLoopStart = targetTime % msPerLoop;
					const timeToNextLoop =
						timeSinceLoopStart === 0 ? 0 : msPerLoop - timeSinceLoopStart;
					serverTargetEpoch = targetTime + timeToNextLoop;
				} else {
					const nowEpoch = performance.timeOrigin + performance.now();
					const nowServer = clockSync.localToServer(nowEpoch);
					const timeSinceLoopStart = nowServer % msPerLoop;
					const timeToNextLoop =
						timeSinceLoopStart === 0 ? 0 : msPerLoop - timeSinceLoopStart;
					serverTargetEpoch = nowServer + timeToNextLoop + 1000;
				}
			}

			db.update(app.jams, jamRows[0].id, { transport_start: new Date(serverTargetEpoch) });
		} finally {
			isStarting = false;
		}
	}

	function initiateStop() {
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;
		db.update(app.jams, jamRows[0].id, { transport_start: null });
	}

	async function playAudioFromServerTime(serverStartTime: number) {
		transport.stop();
		loop.stop();
		loop.cancel();
		transport.position = 0;
		currBeat = 0;

		if (!isContextActive) await startContext();
		if (loadedPlayers.size === 0) return;

		getDestination().volume.rampTo(-Infinity, 0.001);

		const localTargetEpoch = clockSync.serverToLocal(serverStartTime);
		const nowEpoch = performance.timeOrigin + performance.now();
		const delayMs = Math.max(MIN_SCHEDULING_BUFFER_MS, localTargetEpoch - nowEpoch);

		const audioNow = toneNow();
		const startTime = audioNow + delayMs / 1000;

		transportStartTone = startTime;
		countdownMs = nowEpoch + delayMs;

		transport.start(startTime);
		loop.start(0);
		baseBpm = transport.bpm.value;

		setTimeout(
			() => {
				getDestination().volume.rampTo(masterVolume, 0.001);
			},
			Math.max(0, delayMs - 50),
		);

		isPlaying = true;
	}

	function stopAudio() {
		transport.stop();
		loop.stop();
		loop.cancel();
		getDestination().volume.rampTo(-Infinity, 0.001);
		isPlaying = false;
		transportStartTone = null;
		countdownMs = 0;
		currBeat = 0;
		uiBeat = 0;
	}

	async function startContext() {
		await start();
		isContextActive = getContext().state === 'running';
	}

	// ====================
	// Effects
	// ====================

	// Join the jam as a participant (only after the jam row exists locally)
	$effect(() => {
		if (!session) return;
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;

		const userId = session.user_id;

		db.all(app.participants.where({ jam: jamId, user_id: userId })).then((existing) => {
			if (existing.length === 0) {
				const name = localStorage.getItem('wequencer-name') ?? getRandomName();
				localStorage.setItem('wequencer-name', name);
				db.insert(app.participants, {
					jam: jamId,
					user_id: userId,
					display_name: name,
				});
			}
		});
	});

	// Sync BPM from jam row
	$effect(() => {
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;
		const jamBpm = jamRows[0].bpm || DEFAULT_BPM;
		if (jamBpm !== bpm) {
			bpm = jamBpm;
			baseBpm = jamBpm;
			if (isPlaying) {
				transport.bpm.rampTo(jamBpm, 0.5);
			} else {
				transport.bpm.value = jamBpm;
			}
		}
	});

	// Sync beat count from jam row
	$effect(() => {
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;
		const jamBeatCount = jamRows[0].beat_count || DEFAULT_BEAT_COUNT;
		if (jamBeatCount !== beatCount) {
			beatCount = jamBeatCount;
		}
	});

	// Sync playback state from server
	$effect(() => {
		const jamRows = jam.current ?? [];
		if (jamRows.length === 0) return;

		const transportStartDate = jamRows[0].transport_start;
		const serverTransportStart = transportStartDate?.getTime() ?? null;
		const nowPlaying = serverTransportStart != null;

		if (nowPlaying && !wasPlaying) {
			if (serverTransportStart !== lastProcessedTransportStart) {
				lastProcessedTransportStart = serverTransportStart;
				playAudioFromServerTime(serverTransportStart);
			}
		} else if (!nowPlaying && wasPlaying) {
			lastProcessedTransportStart = null;
			stopAudio();
		}
		wasPlaying = nowPlaying;
	});

	// Load instrument audio players from Jazz sound data once the audio context is active
	$effect(() => {
		if (!isContextActive) return;
		const currentInstruments = instruments.current ?? [];
		if (currentInstruments.length === 0) return;

		untrack(() => {
			for (const instrument of currentInstruments) {
				if (loadedPlayers.has(instrument.id)) continue;

				// Bytea comes back as plain Array due to JSON deserialization (mai-ammr)
				const soundData = instrument.sound;
				if (!soundData || (soundData as unknown as unknown[]).length === 0) continue;
				const bytes = new Uint8Array(soundData as unknown as ArrayLike<number>);
				const blob = new Blob([bytes]);
				const url = URL.createObjectURL(blob);

				loadedPlayers.set(instrument.id, {
					instrumentId: instrument.id,
					player: undefined,
				});

				loadPlayer(url, instrument.id)
					.then((player) => {
						loadedPlayers.set(instrument.id, {
							instrumentId: instrument.id,
							player,
						});
					})
					.catch((err) => {
						console.error(`Failed to load player for ${instrument.name}:`, err);
						URL.revokeObjectURL(url);
					});
			}
		});
	});

	// Monitor audio context state
	$effect(() => {
		const checkContext = () => {
			isContextActive = getContext().state === 'running';
		};
		checkContext();
		const interval = setInterval(checkContext, 100);
		return () => clearInterval(interval);
	});

	// Drift correction
	$effect(() => {
		if (!isPlaying || transportStartTone === null) return;

		const driftCheckInterval = setInterval(() => {
			if (isSyncing || transportStartTone === null) return;

			const nowTone = toneNow();
			const transportTimeMs = transport.seconds * 1000;
			const expectedElapsedMs = (nowTone - transportStartTone) * 1000;

			if (
				expectedElapsedMs < 0 ||
				!Number.isFinite(expectedElapsedMs) ||
				!Number.isFinite(transportTimeMs) ||
				Math.abs(expectedElapsedMs) > 1000 * 60 * 60 * 24
			) {
				driftCount = 0;
				return;
			}

			const rawDrift = expectedElapsedMs - transportTimeMs;

			if (Math.abs(rawDrift) <= DRIFT_THRESHOLD_MS) {
				driftCount = 0;
				return;
			}

			driftCount++;
			if (driftCount < DRIFT_HYSTERESIS_COUNT) return;

			isSyncing = true;

			const maxRelativeChange = 0.02;
			const relativeChange = Math.max(
				-maxRelativeChange,
				Math.min(maxRelativeChange, rawDrift / 10000),
			);
			const targetBpm = baseBpm * (1 + relativeChange);
			const clampedBpm = Math.max(10, Math.min(400, targetBpm));

			if (Number.isFinite(clampedBpm)) {
				transport.bpm.rampTo(clampedBpm, 0.1);
			}

			driftCount = 0;
			isSyncing = false;
		}, DRIFT_CHECK_INTERVAL_MS);

		return () => clearInterval(driftCheckInterval);
	});

	// Easter egg
	function easterEgg() {
		easterEggCounter++;
		if (easterEggCounter < 7) return;

		const lick = [
			{ pitch: 'D4', dur: '16n' },
			{ pitch: 'E4', dur: '16n' },
			{ pitch: 'F4', dur: '16n' },
			{ pitch: 'G4', dur: '16n' },
			{ pitch: 'E4', dur: '8n' },
			{ pitch: 'C4', dur: '16n' },
			{ pitch: 'D4', dur: '4n' },
		];

		const synth = new Synth().toDestination();
		const nowT = toneNow();
		let durSoFar = 0;

		lick.forEach((note) => {
			synth.triggerAttackRelease(note.pitch, note.dur, nowT + durSoFar);
			durSoFar += Time(note.dur).toSeconds();
		});
		easterEggCounter = 0;
	}

	// ====================
	// Mixer Controls
	// ====================

	function setMasterVolume(db: number) {
		const clamped = Math.max(-60, Math.min(6, db));
		masterVolume = clamped;
		getDestination().volume.rampTo(clamped, 0.05);
	}

	// Track per-instrument volume in dB (Gain node uses linear internally)
	const instrumentVolumes = new Map<string, number>();

	function getInstrumentVolume(instrumentId: string): number {
		return instrumentVolumes.get(instrumentId) ?? 0;
	}

	function setInstrumentVolume(instrumentId: string, db: number) {
		const strip = getChannelStrip(instrumentId);
		const clamped = Math.max(-60, Math.min(6, db));
		instrumentVolumes.set(instrumentId, clamped);
		const linear = clamped <= -60 ? 0 : Math.pow(10, clamped / 20);
		strip.gain.gain.rampTo(linear, 0.05);
	}

	function getInstrumentPan(instrumentId: string): number {
		const strip = channelStrips.get(instrumentId);
		return strip ? strip.panner.pan.value : 0;
	}

	function setInstrumentPan(instrumentId: string, pan: number) {
		const strip = getChannelStrip(instrumentId);
		const clamped = Math.max(-1, Math.min(1, pan));
		strip.panner.pan.rampTo(clamped, 0.05);
	}

	// ====================
	// Context Provider
	// ====================

	const audioContext = $state({
		play: initiatePlay,
		stop: initiateStop,
		startContext,
		easterEgg,
		get uiBeat() {
			return uiBeat;
		},
		get isPlaying() {
			return isPlaying;
		},
		get isContextActive() {
			return isContextActive;
		},
		get countdownMs() {
			return countdownMs;
		},
		get bpm() {
			return bpm;
		},
		setBpm,
		get syncAlignment() {
			return syncAlignment;
		},
		setSyncAlignment,
		get beatCount() {
			return beatCount;
		},
		setBeatCount,
		get masterVolume() {
			return masterVolume;
		},
		setMasterVolume,
		getInstrumentVolume,
		setInstrumentVolume,
		getInstrumentPan,
		setInstrumentPan,
	});

	setAudioContext(audioContext);

	// Cleanup
	onDestroy(() => {
		cleanUpPlayers();
		clockSync.destroy();
	});
</script>

{@render children?.()}
