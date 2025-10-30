<script lang="ts">
  import type { co } from "jazz-tools";
  import { announceBand } from "$lib/announceBandSchema.svelte.ts";
  import type { BandList } from "./schema";
  let bandName = $state("");
  let bandList = $state<co.loaded<typeof BandList, { $each: true }>>();
  async function handleAnnounceBand() {
    const bandListResponse = await announceBand.send({
      band: { name: bandName },
    });
    bandName = "";
    if (bandListResponse.bandList) {
      bandList = bandListResponse.bandList;
    }
  }
</script>

<div>
  <input type="text" bind:value={bandName} />
  <button type="button" onclick={handleAnnounceBand}> Announce Band </button>
  <div>
    {#if bandList?.$isLoaded}
      {#each bandList as band (band?.$jazz.id)}
        <div>{band.name}</div>
      {/each}
    {/if}
  </div>
</div>
