<script lang="ts" module>
  export type Props = {
    id: string;
  };
</script>

<script lang="ts">
  import { CoState } from '../../../jazz.class.svelte.js';
  import { Person } from './schema.js';

  let props: Props = $props();

  const person = new CoState(Person, () => props.id, {
    resolve: {
      dog: true
    }
  })
</script>

<!-- Using non-null assertions because we want to test that locally available values are never null -->
<label>
  Name
  <!-- Jazz values are reactive, but they are not recognized as reactive by Svelte -->
  <!-- svelte-ignore binding_property_non_reactive -->
  <input type="text" bind:value={person.current!.name} />
</label>

<label>
  Dog
   <!-- Jazz values are reactive, but they are not recognized as reactive by Svelte -->
  <!-- svelte-ignore binding_property_non_reactive -->
  <input type="text" bind:value={person.current!.dog.name} />
</label>

<div data-testid="person-name">{person.current!.name}</div>
<div data-testid="person-dog-name">{person.current!.dog.name}</div>
