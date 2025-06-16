<script lang="ts">
  import { AccountCoState, CoState } from '$lib/jazz.class.svelte.js';
  import { VList } from 'virtua/svelte';
  import { TestAccount, People } from './schema.js';

  const me = new AccountCoState(TestAccount, {
    resolve: {
      root: {
        people: true
      }
    }
  });


  const keys = $derived(Object.keys(me.current?.root.people._refs ?? {}));
  let currentKey = $state("");
  const currentListId = $derived(me.current?.root.people._refs[currentKey]?.id ?? null);


  const list = new CoState(People, () => currentListId, {
    resolve: {
      $each: {
        dog: true
      }
    }
  });

  let filter = $state<string>('');

  const data = $derived(list.current?.filter(item => item.name.toLowerCase().includes(filter.toLowerCase())) ?? []);
</script>

<label>
  <span>List</span>

  <select bind:value={currentKey}>
    {#each keys as key}
      <option value={key}>{key}</option>
    {/each}
  </select>
</label>

<label>
  <span>Filter</span>
  <input bind:value={filter} placeholder="Filter" />
</label>

<VList {data} style="height: 100vh;" getKey={(val) => val.id}>
  {#snippet children(item)}
    <div
      style="
        height: 75px;
        background: white;
        display: flex;
        flex-direction: column;
        justify-content: center;
        padding: 10px;
      "
    >
      <div>Name: {item.name}</div>
      <div>Age: {item.age}</div>
      <div>Dog: {item.dog.name}</div>
    </div>
  {/snippet}
</VList>