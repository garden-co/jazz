<script lang="ts">
  import { AccountCoState, CoState } from 'jazz-tools/svelte';
  import { TestAccount, Person } from './schema.js';

  const me = new AccountCoState(TestAccount, {
    resolve: {
      root: {
        people: true
      }
    }
  });

  let id = $state<string>();
  let customId = $state<string>();

  const person = new CoState(Person, () => id, {
    resolve: { dog: true }
  });
</script>

<div>
  {#each me.current?.root?.people?.$jazz.refs || [] as ref, index}
    <button
      onclick={() => {
        id = ref.id;
      }}>Select person [{index}]</button
    >
  {/each}
  <button
    onclick={() => {
      console.log(person.current?.name);
    }}>Log person</button
  >
  <button
    onclick={() => {
      id = undefined;
    }}>Set undefined</button
  >
  <button
    onclick={() => {
      id = customId;
    }}>Set customId</button
  >

  <label>
    Custom ID
    <input type="text" bind:value={customId} />
  </label>

  <button
    onclick={() => {
      me.logOut();
    }}>Logout</button
  >
</div>

{#if person.current}
  <div>
    <label>
      Name
      <!-- Jazz values are reactive, but they are not recognized as reactive by Svelte -->
      <!-- svelte-ignore binding_property_non_reactive -->
      <input type="text" bind:value={
        () => person.current!.name,
        newValue => person.current!.$jazz.set("name", newValue)
      } />
    </label>
    <label>
      Dog
      <!-- Jazz values are reactive, but they are not recognized as reactive by Svelte -->
      <!-- svelte-ignore binding_property_non_reactive -->
      <input type="text" bind:value={
        () => person.current!.dog.name,
        newValue => person.current!.dog.$jazz.set("name", newValue)
      } />
    </label>
    <div data-testid="person-name">{person.current.name}</div>
    <div data-testid="person-dog-name">{person.current.dog.name}</div>
  </div>
{/if}

<style>
  button:hover {
    text-decoration: underline;
  }
</style>
