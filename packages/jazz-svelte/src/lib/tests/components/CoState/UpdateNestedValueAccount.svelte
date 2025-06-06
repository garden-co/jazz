<script lang="ts">
  import { AccountCoState } from '../../../jazz.class.svelte.js';

  import { MyAccount } from './schema.js';

  const me = new AccountCoState(MyAccount, {
    resolve: {
      root: {
        dog: true
      }
    }
  });

  const dogName = $derived(me.current!.root.dog.name);
</script>

<!-- Using non-null assertions because we want to test that locally available values are never null -->
<label>
  Name
  <!-- Jazz values are reactive, but they are not recognized as reactive by Svelte -->
  <!-- svelte-ignore binding_property_non_reactive -->
  <input type="text" bind:value={me.current!.root.name} />
</label>

<label>
  Dog
  <!-- Jazz values are reactive, but they are not recognized as reactive by Svelte -->
  <!-- svelte-ignore binding_property_non_reactive -->
  <input type="text" bind:value={me.current!.root.dog.name} />
</label>

<div data-testid="person-name">{me.current!.root.name}</div>
<div data-testid="person-dog-name">{dogName}</div>
