<script lang="ts" module>
  export type Props<
    S extends (AccountClass<Account> & CoValueFromRaw<Account>) | AnyAccountSchema
  > = JazzContextManagerProps<S> & {
    children?: Snippet;
  };
</script>

<script
  lang="ts"
  generics="S extends
| (AccountClass<Account> & CoValueFromRaw<Account>)
| AnyAccountSchema,"
>
  import { JazzBrowserContextManager, type JazzContextManagerProps } from 'jazz-browser';
  import type { AuthSecretStorage, InstanceOfSchema } from 'jazz-tools';
  import { Account, type AccountClass, type CoValueFromRaw, type AnyAccountSchema } from 'jazz-tools';
  import { type Snippet, setContext, untrack } from 'svelte';
  import { JAZZ_AUTH_CTX, JAZZ_CTX, type JazzContext } from './jazz.svelte.js';

  let props: Props<S> = $props();

  const contextManager = new JazzBrowserContextManager<S>();

  const ctx = $state<JazzContext<InstanceOfSchema<S>>>({ current: undefined });
  setContext<JazzContext<InstanceOfSchema<S>>>(JAZZ_CTX, ctx);
  setContext<AuthSecretStorage>(JAZZ_AUTH_CTX, contextManager.getAuthSecretStorage());

  $effect(() => {
    props.sync.when;
    props.sync.peer;
    props.storage;
    props.guestMode;
    return untrack(() => {
      if (!props.sync) return;

      contextManager
        .createContext({
          sync: props.sync,
          storage: props.storage,
          guestMode: props.guestMode,
          AccountSchema: props.AccountSchema,
          defaultProfileName: props.defaultProfileName,
          onAnonymousAccountDiscarded: props.onAnonymousAccountDiscarded,
          onLogOut: props.onLogOut
        })
        .catch((error) => {
          console.error('Error creating Jazz browser context:', error);
        });
    });
  });

  $effect(() => {
    return contextManager.subscribe(() => {
      ctx.current = contextManager.getCurrentValue();
    });
  });
</script>

{#if ctx.current}
  {@render props.children?.()}
{/if}
