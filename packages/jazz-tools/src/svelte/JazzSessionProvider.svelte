<script lang="ts">
  import { onMount, type Snippet } from 'svelte';
  import { createJazzSession, type JazzSessionConfig } from '../session/create-jazz-session.js';
  import type { JazzSession, JazzSessionSnapshot } from '../session/state.js';
  import type { JazzClient } from './create-jazz-client.js';
  import JazzSessionClientProvider from './JazzSessionClientProvider.svelte';
  import { setJazzSession } from './session-state.js';

  type Props = {
    children: Snippet;
    fallback?: Snippet;
    autoAttachDevTools?: boolean;
  } & ({ session: JazzSession<JazzClient>; config?: never } | { config: JazzSessionConfig; session?: never });
  let { session, config, children, fallback, autoAttachDevTools = true }: Props = $props();
  let owned = $state.raw<JazzSession<JazzClient>>();
  let cancelled = false;
  let attempt: Promise<void> | undefined;
  let created: JazzSession<JazzClient> | undefined;
  let startup: JazzSessionSnapshot<JazzClient> = { status: 'transitioning' };
  const listeners = new Set<() => void>();
  const unavailable = async () => { throw new Error('Jazz session is not ready; retry initialization first'); };
  const publish = (snapshot: JazzSessionSnapshot<JazzClient>) => {
    startup = Object.freeze(snapshot);
    for (const listener of [...listeners]) listener();
  };
  const start = (): Promise<void> => {
    if (cancelled) return Promise.reject(new Error('Jazz session is closed'));
    if (attempt) return attempt;
    if (!config) return Promise.resolve();
    publish({ status: 'transitioning' });
    attempt = createJazzSession(config).then(async (value) => {
      created = value;
      if (cancelled) await value.close();
      else owned = value;
    }).catch((cause) => {
      attempt = undefined;
      if (!cancelled) publish({ status: 'error', error: cause instanceof Error ? cause : new Error(String(cause)) });
      throw cause;
    });
    return attempt;
  };
  // The startup fallback has the same hook context, including retry, before any
  // runtime is ready. The active inner provider replaces it with the real owner.
  setJazzSession({
    getSnapshot: () => startup,
    subscribe: (listener) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    createLocalFirst: unavailable, restoreLocalFirst: unavailable,
    registerJWT: unavailable, loginJWT: unavailable, linkJWT: unavailable,
    logout: unavailable, retry: start,
    close: async () => { cancelled = true; publish({ status: 'closed' }); await created?.close(); },
  });
  onMount(() => {
    if (!config) return;
    void start().catch(() => {});
    return () => { cancelled = true; if (created) void created.close().catch(console.error); };
  });
</script>

{#if session ?? owned}
  {@const owner = (session ?? owned)!}
  {#key owner}
    <JazzSessionClientProvider session={owner} {children} {fallback} {autoAttachDevTools} />
  {/key}
{:else}
  {@render fallback?.()}
{/if}
