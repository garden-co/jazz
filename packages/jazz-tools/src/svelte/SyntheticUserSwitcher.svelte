<script lang="ts">
	import type { LocalAuthMode } from '../runtime/context.js';
	import {
		createSyntheticUserProfile,
		loadSyntheticUserStore,
		saveSyntheticUserStore,
		setActiveSyntheticProfile,
		type SyntheticUserProfile,
		type SyntheticUserStorageOptions,
		type SyntheticUserStore
	} from '../synthetic-users.js';

	interface Props extends SyntheticUserStorageOptions {
		appId: string;
		class?: string;
		reloadOnSwitch?: boolean;
		onProfileChange?: (profile: SyntheticUserProfile) => void;
	}

	let {
		appId,
		class: className,
		reloadOnSwitch = true,
		onProfileChange,
		storage,
		storageKey,
		defaultMode
	}: Props = $props();

	let storageOptions = $derived({ storage, storageKey, defaultMode });

	let store = $state<SyntheticUserStore>(undefined!);

	$effect.pre(() => {
		store = loadSyntheticUserStore(appId, storageOptions);
	});

	function getActiveProfile(s: SyntheticUserStore): SyntheticUserProfile {
		return s.profiles.find((p) => p.id === s.activeProfileId) ?? s.profiles[0];
	}

	let activeProfile = $derived(getActiveProfile(store));

	function applyStore(nextStore: SyntheticUserStore, triggerReload: boolean) {
		saveSyntheticUserStore(appId, nextStore, storageOptions);
		store = nextStore;
		onProfileChange?.(getActiveProfile(nextStore));
		if (triggerReload && reloadOnSwitch && typeof window !== 'undefined') {
			window.location.reload();
		}
	}

	function handleSwitch(event: Event) {
		const target = event.target as HTMLSelectElement;
		const nextStore = setActiveSyntheticProfile(appId, target.value, storageOptions);
		store = nextStore;
		onProfileChange?.(getActiveProfile(nextStore));
		if (reloadOnSwitch && typeof window !== 'undefined') {
			window.location.reload();
		}
	}

	function handleModeChange(event: Event) {
		const target = event.target as HTMLSelectElement;
		const mode = target.value as LocalAuthMode;
		const nextStore: SyntheticUserStore = {
			...store,
			profiles: store.profiles.map((profile) =>
				profile.id === store.activeProfileId ? { ...profile, mode } : profile
			)
		};
		applyStore(nextStore, false);
	}

	function handleAddProfile() {
		const suggestedName = `User ${store.profiles.length + 1}`;
		const rawName =
			typeof window !== 'undefined'
				? window.prompt('New synthetic user name', suggestedName)
				: suggestedName;
		if (rawName === null) return;
		const name = rawName.trim() || suggestedName;
		const profile = createSyntheticUserProfile(name, 'demo');
		const nextStore: SyntheticUserStore = {
			activeProfileId: profile.id,
			profiles: [...store.profiles, profile]
		};
		applyStore(nextStore, true);
	}

	function handleRemoveProfile() {
		if (store.profiles.length <= 1) return;
		const nextProfiles = store.profiles.filter((p) => p.id !== store.activeProfileId);
		const nextStore: SyntheticUserStore = {
			activeProfileId: nextProfiles[0].id,
			profiles: nextProfiles
		};
		applyStore(nextStore, true);
	}
</script>

<div class={className}>
	<label>
		Synthetic User
		<select value={store.activeProfileId} onchange={handleSwitch}>
			{#each store.profiles as profile (profile.id)}
				<option value={profile.id}>{profile.name} ({profile.mode})</option>
			{/each}
		</select>
	</label>
	{' '}
	<label>
		Mode
		<select value={activeProfile.mode} onchange={handleModeChange}>
			<option value="anonymous">anonymous</option>
			<option value="demo">demo</option>
		</select>
	</label>
	{' '}
	<button type="button" onclick={handleAddProfile}>Add</button>
	{' '}
	<button type="button" disabled={store.profiles.length <= 1} onclick={handleRemoveProfile}>
		Remove
	</button>
</div>
