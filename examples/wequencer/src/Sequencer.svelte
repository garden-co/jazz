<script lang="ts">
  import { QuerySubscription } from "jazz-tools/svelte";
  import { app } from "../schema/app.js";
  import { getAudioContext } from "./audio-context.js";
  import InstrumentRow from "./InstrumentRow.svelte";

  const audio = getAudioContext();

  let { jamId }: { jamId: string } = $props();

  const instruments = new QuerySubscription(
    app.instruments.orderBy("display_order"),
  );
  const allBeats = new QuerySubscription(app.beats);

  function beatsForInstrument(instrumentId: string) {
    return (allBeats.current ?? []).filter(
      (b) => b.jam === jamId && b.instrument === instrumentId,
    );
  }
</script>

<div class="sequencer">
  <div class="master-volume">
    <label class="master-label" for="master-volume">Master</label>
    <input
      id="master-volume"
      class="vol-slider"
      type="range"
      min="-60"
      max="6"
      step="1"
      value={audio.masterVolume}
      oninput={(e) => audio.setMasterVolume(Number(e.currentTarget.value))}
    />
  </div>
  <div class="grid">
    {#each instruments.current ?? [] as instrument (instrument.id)}
      <InstrumentRow
        {jamId}
        {instrument}
        beats={beatsForInstrument(instrument.id)}
      />
    {/each}
  </div>
</div>
