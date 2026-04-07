<script lang="ts">
  import { getDb, getSession } from "jazz-tools/svelte";
  import { app, type Beat, type Instrument } from "../schema.js";
  import { getStableHue } from "./constants.js";
  import { getAudioContext } from "./audio-context.js";

  let {
    jamId,
    instrument,
    beats,
  }: {
    jamId: string;
    instrument: Instrument;
    beats: Beat[];
  } = $props();

  const db = getDb();
  const session = getSession();
  const audio = getAudioContext();

  const beatIndices = $derived(
    Array.from({ length: audio.beatCount }, (_, i) => i),
  );
  let mixerOpen = $state(false);

  function beatAt(index: number): Beat | undefined {
    return beats.find((b) => b.beat_index === index);
  }

  function toggleBeat(index: number) {
    const existing = beatAt(index);
    if (existing) {
      db.delete(app.beats, existing.id);
    } else {
      db.insert(app.beats, {
        jamId: jamId,
        instrumentId: instrument.id,
        beat_index: index,
        placed_by: session?.user_id ?? "anonymous",
      });
    }
  }
</script>

<div class="instrument-row">
  <div class="instrument-beat-row">
    <button
      class="instrument-name"
      class:mixer-open={mixerOpen}
      onclick={() => (mixerOpen = !mixerOpen)}
    >
      {instrument.name}
    </button>
    <div class="beat-cells">
      {#each beatIndices as index (index)}
        {@const beat = beatAt(index)}
        {@const hue = beat ? getStableHue(beat.placed_by) : undefined}
        {@const isCurrentBeat = audio.uiBeat === index && audio.isPlaying}
        <button
          class="beat-cell"
          class:downbeat={index % 4 === 0}
          class:active={beat != null}
          class:current={isCurrentBeat}
          style:background-color={isCurrentBeat && hue != null
            ? `oklch(60% 0.18 ${hue})`
            : hue != null
              ? `oklch(50% 0.12 ${hue})`
              : ""}
          style:box-shadow={isCurrentBeat && hue != null
            ? `inset 0 1px 2px rgba(0,0,0,0.3), 0 0 8px oklch(60% 0.18 ${hue} / 0.5)`
            : ""}
          onclick={() => toggleBeat(index)}
          aria-label="beat {index} {instrument.name}"
        ></button>
      {/each}
    </div>
  </div>

  {#if mixerOpen}
    <div class="instrument-mixer">
      <label class="mixer-label" for="vol-{instrument.id}">Vol</label>
      <input
        id="vol-{instrument.id}"
        class="vol-slider"
        type="range"
        min="-60"
        max="6"
        step="1"
        value={audio.getInstrumentVolume(instrument.id)}
        oninput={(e) =>
          audio.setInstrumentVolume(
            instrument.id,
            Number(e.currentTarget.value),
          )}
      />
      <label class="mixer-label" for="pan-{instrument.id}">Pan</label>
      <input
        id="pan-{instrument.id}"
        class="pan-slider"
        type="range"
        min="-100"
        max="100"
        step="5"
        value={Math.round(audio.getInstrumentPan(instrument.id) * 100)}
        oninput={(e) =>
          audio.setInstrumentPan(
            instrument.id,
            Number(e.currentTarget.value) / 100,
          )}
      />
    </div>
  {/if}
</div>
