<script lang="ts">
  import type { AnyAccountSchema, ResolveQuery } from "jazz-tools";
  import { AccountCoState } from "../jazz.class.svelte";

  type Props<A extends AnyAccountSchema, R extends ResolveQuery<A>> = {
    Schema: A;
    options?: { resolve?: R };
  };

  let { Schema, options }: Props<any, any> = $props();

  const state = new AccountCoState(Schema, options);
</script>

<div data-testid="account-costate-wrapper">
  <div data-testid="loading-state">{state.current.$jazz.loadingState}</div>
  <div data-testid="is-loaded">{state.current.$isLoaded ? "true" : "false"}</div>
  {#if state.current.$isLoaded}
    <div data-testid="account-id">{state.current.$jazz.id}</div>
    <div data-testid="state-json">{JSON.stringify(state.current.toJSON())}</div>
  {/if}
</div>


