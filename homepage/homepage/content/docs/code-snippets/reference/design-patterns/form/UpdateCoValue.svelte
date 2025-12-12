<script lang="ts">
  // [!code hide:5]
  import { co, z } from "jazz-tools";
  const Order = co.map({
    name: z.string(),
  });
  import { CoState } from 'jazz-tools/svelte';
  // @ts-expect-error
  const order = new CoState(orderId);
</script>

<input
  type="text"
  bind:value={
    () => order.current.name, (v) => order.current.$jazz.set("name", v)
  }
/>
