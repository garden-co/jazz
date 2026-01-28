<script lang="ts">
  import { co } from "jazz-tools";
  import { BubbleTeaOrder, PartialBubbleTeaOrder } from "./schema";
  const { order, onSave }:  {
    order: co.loaded<typeof BubbleTeaOrder> | co.loaded<typeof PartialBubbleTeaOrder>;
    onSave?: (evt: Event) => void;
  } = $props();
</script>

<form onsubmit={onSave || ((evt) => evt.preventDefault())}>
  <label>
    Name
    <input
      type="text"
      bind:value={() => order.name, (v) => v && order.$jazz.set("name", v)}
      required
    />
  </label>

  {#if onSave}
  <button type="submit">Submit</button>
  {/if}
</form>
