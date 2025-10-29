
<script lang="ts">
  import { AccountCoState } from "jazz-tools/svelte";
  import { MyAppAccount } from "./schema";
  const account = new AccountCoState(MyAppAccount, {
    resolve: { profile: true }
  });
</script>

{#if !account.current.$isLoaded}
  Loading...
{:else}
  <div>
    <h1>{account.current.profile.name}'s projects</h1>
  </div>
{/if}
