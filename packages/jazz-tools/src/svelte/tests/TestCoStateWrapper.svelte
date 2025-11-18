<script lang="ts">
  import type { CoValueClassOrSchema, ResolveQuery } from 'jazz-tools';
  import { CoState } from '../jazz.class.svelte';

  type Props<V extends CoValueClassOrSchema, R extends ResolveQuery<V>> = {
    Schema: V;
    id: string;
    options?: { resolve?: R };
  };

  let { Schema, id, options }: Props<any, any> = $props();

  const state = new CoState(Schema, id, options);
</script>

<div data-testid="costate-wrapper">
  <div data-testid="loading-state">{state.current.$jazz.loadingState}</div>
  <div data-testid="is-loaded">{state.current.$isLoaded ? 'true' : 'false'}</div>
  {#if state.current.$isLoaded}
    <div data-testid="state-value">{JSON.stringify(state.current.toJSON())}</div>
  {/if}
</div>

