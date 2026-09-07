<script lang="ts">
  import { onMount, type Snippet } from 'svelte';
  import { createJazzSession, type JazzSessionConfig } from '../session/create-jazz-session.js';
  import type { JazzSession } from '../session/state.js';
  import type { JazzClient } from './create-jazz-client.js';
  import JazzSessionClientProvider from './JazzSessionClientProvider.svelte';

  type Props = {
    children: Snippet;
    fallback?: Snippet;
    autoAttachDevTools?: boolean;
  } & ({ session: JazzSession<JazzClient>; config?: never } | { config: JazzSessionConfig; session?: never });
  let { session, config, children, fallback, autoAttachDevTools = true }: Props = $props();
  let owned = $state<JazzSession<JazzClient>>();
  let error = $state<Error>();
  onMount(() => {
    if (!config) return;
    let cancelled = false;
    let created: JazzSession<JazzClient> | undefined;
    void createJazzSession(config).then(async (value) => {
      created = value;
      if (cancelled) await value.close();
      else owned = value;
    }).catch((cause) => { if (!cancelled) error = cause instanceof Error ? cause : new Error(String(cause)); });
    return () => { cancelled = true; if (created) void created.close().catch(console.error); };
  });
</script>

{#if error}
  {(() => { throw error; })()}
{:else if session ?? owned}
  {@const owner = (session ?? owned)!}
  {#key owner}
    <JazzSessionClientProvider session={owner} {children} {fallback} {autoAttachDevTools} />
  {/key}
{:else}
  {@render fallback?.()}
{/if}
