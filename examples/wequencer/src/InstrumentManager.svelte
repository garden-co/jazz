<script lang="ts">
	import { getDb, QuerySubscription } from 'jazz-tools/svelte';
	import { app } from '../schema.js';

	const db = getDb();
	const instruments = new QuerySubscription(app.instruments.orderBy('display_order'));

	let showForm = $state(false);
	let name = $state('');
	let file = $state<File | null>(null);
	let uploading = $state(false);

	async function addInstrument() {
		if (!name.trim() || !file) return;
		uploading = true;

		try {
			const storedFile = await db.createFileFromBlob(app, file, { tier: 'edge' });
			const maxOrder = Math.max(0, ...(instruments.current ?? []).map((i) => i.display_order));
			db.insert(app.instruments, {
				name: name.trim(),
				soundFileId: storedFile.id,
				display_order: maxOrder + 1,
			});
			name = '';
			file = null;
			showForm = false;
		} finally {
			uploading = false;
		}
	}

	async function removeInstrument(id: string) {
		const instrument = await db.one(app.instruments.where({ id }));
		const fileId = instrument?.soundFileId;
		if (fileId) {
			const storedFile = await db.one(app.files.where({ id: fileId }));
			if (storedFile) {
				for (const partId of storedFile.partIds) {
					db.delete(app.file_parts, partId);
				}
				db.delete(app.files, storedFile.id);
			}
		}

		db.delete(app.instruments, id);
	}
</script>

<section class="instrument-manager">
	<div class="manager-header">
		<h3>Instruments</h3>
		<button class="toggle-form-btn" onclick={() => (showForm = !showForm)}>
			{showForm ? 'Cancel' : '+ Add'}
		</button>
	</div>

	{#if showForm}
		<form class="add-form" onsubmit={(e) => { e.preventDefault(); addInstrument(); }}>
			<input
				type="text"
				placeholder="Instrument name"
				bind:value={name}
				class="name-input"
			/>
			<label class="file-input">
				<span>{file ? file.name : 'Choose audio file'}</span>
				<input
					type="file"
					accept="audio/*"
					onchange={(e) => { file = e.currentTarget.files?.[0] ?? null; }}
				/>
			</label>
			<button type="submit" class="upload-btn" disabled={!name.trim() || !file || uploading}>
				{uploading ? 'Uploading...' : 'Add'}
			</button>
		</form>
	{/if}

	<ul class="instrument-list">
		{#each instruments.current ?? [] as instrument (instrument.id)}
			<li>
				<span class="inst-name">{instrument.name}</span>
				<button class="remove-btn" onclick={() => removeInstrument(instrument.id)} title="Remove">
					&times;
				</button>
			</li>
		{/each}
	</ul>
</section>

<style>
	.instrument-manager {
		background: #2a2420;
		border: 2px solid #3d3530;
		border-radius: 0.5rem;
		padding: 1rem;
		min-width: 200px;
		box-shadow:
			0 4px 24px rgba(0, 0, 0, 0.4),
			inset 0 1px 0 rgba(255, 255, 255, 0.03);
	}

	.manager-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		margin-bottom: 0.75rem;
	}

	h3 {
		font-family: 'Space Grotesk', system-ui, sans-serif;
		font-size: 1.1rem;
		font-weight: 700;
		color: #f0a030;
		text-transform: uppercase;
		letter-spacing: 0.04em;
	}

	.toggle-form-btn {
		font-family: 'Space Grotesk', system-ui, sans-serif;
		font-size: 0.8rem;
		font-weight: 700;
		text-transform: uppercase;
		letter-spacing: 0.04em;
		padding: 0.3rem 0.75rem;
		border: 1px solid #3d3530;
		border-radius: 0.25rem;
		background: #1c1816;
		color: #f0a030;
		cursor: pointer;
		transition: background-color 0.15s;
	}

	.toggle-form-btn:hover {
		background: #352e28;
	}

	.add-form {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
		margin-bottom: 0.75rem;
		padding-bottom: 0.75rem;
		border-bottom: 1px solid #3d3530;
	}

	.name-input {
		font-family: system-ui, sans-serif;
		font-size: 0.85rem;
		padding: 0.4rem 0.5rem;
		border: 1px solid #3d3530;
		border-radius: 0.25rem;
		background: #1c1816;
		color: #e8e0d8;
		outline: none;
		transition: border-color 0.15s;
	}

	.name-input:focus {
		border-color: #f0a030;
	}

	.name-input::placeholder {
		color: #6b5f55;
	}

	.file-input {
		display: block;
		font-size: 0.8rem;
		padding: 0.4rem 0.5rem;
		border: 1px dashed #3d3530;
		border-radius: 0.25rem;
		background: #1c1816;
		color: #9a8e82;
		cursor: pointer;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		transition: border-color 0.15s;
	}

	.file-input:hover {
		border-color: #6b5f55;
	}

	.file-input input {
		display: none;
	}

	.upload-btn {
		font-family: 'Space Grotesk', system-ui, sans-serif;
		font-size: 0.8rem;
		font-weight: 700;
		text-transform: uppercase;
		letter-spacing: 0.04em;
		padding: 0.4rem 0.75rem;
		border: none;
		border-radius: 0.25rem;
		background: #f0a030;
		color: #1c1816;
		cursor: pointer;
		transition: opacity 0.15s;
	}

	.upload-btn:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.instrument-list {
		list-style: none;
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
	}

	li {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 0.3rem 0.4rem;
		border-radius: 0.25rem;
		transition: background-color 0.15s;
	}

	li:hover {
		background: rgba(240, 160, 48, 0.06);
	}

	.inst-name {
		font-size: 0.85rem;
		font-weight: 600;
	}

	.remove-btn {
		font-size: 1rem;
		line-height: 1;
		padding: 0.15rem 0.4rem;
		border: none;
		border-radius: 0.25rem;
		background: transparent;
		color: #6b5f55;
		cursor: pointer;
		transition: color 0.15s;
	}

	.remove-btn:hover {
		color: #dc4040;
	}
</style>
