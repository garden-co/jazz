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
</script>

<!-- Using non-null assertions because we want to test that locally available values are never null -->
<label>
  Name
  <input type="text" bind:value={me.current!.root.name} />
</label>

<label>
  Dog
  <input type="text" bind:value={me.current!.root.dog.name} />
</label>

<div data-testid="person-name">{me.current!.root.name}</div>
<div data-testid="person-dog-name">{me.current!.root.dog.name}</div>
