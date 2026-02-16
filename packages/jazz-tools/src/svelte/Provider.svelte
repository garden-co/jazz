<script
  lang="ts"
  generics="S extends (AccountClass<Account> & CoValueFromRaw<Account>) | AnyAccountSchema"
>
  import type { AuthSecretStorage, InstanceOfSchema } from "jazz-tools";
  import {
    Account,
    type AccountClass,
    type AnyAccountSchema,
    type CoValueFromRaw,
  } from "jazz-tools";
  import {
    JazzBrowserContextManager,
    type JazzContextManagerProps,
  } from "jazz-tools/browser";
  import { type Snippet, setContext, untrack } from "svelte";
  import { JAZZ_AUTH_CTX, JAZZ_CTX, type JazzContext } from "./jazz.svelte.js";

  let props: JazzContextManagerProps<S> & {
    children?: Snippet;
    enableSSR?: boolean;
    authSecretStorageKey?: string;
    navigation?: {
      beforeNavigate: (
        callback: (navigation: any) => void | Promise<void>,
      ) => void;
      goto: (url: string, opts?: any) => Promise<void>;
    };
  } = $props();

  const contextManager = new JazzBrowserContextManager<S>({
    useAnonymousFallback: props.enableSSR,
    authSecretStorageKey: props.authSecretStorageKey,
  });

  const ctx = $state<JazzContext<InstanceOfSchema<S>>>({ current: undefined });

  setContext<JazzContext<InstanceOfSchema<S>>>(JAZZ_CTX, ctx);
  setContext<AuthSecretStorage>(
    JAZZ_AUTH_CTX,
    contextManager.getAuthSecretStorage(),
  );

  if (props.navigation) {
    let isReplaying = false;

    props.navigation.beforeNavigate(async (navigation: any) => {
      if (isReplaying || navigation.willUnload) return;

      const current = ctx.current;
      if (!current) return;

      if (current.node.syncManager.unsyncedTracker.isAllSynced()) return;

      navigation.cancel();

      try {
        await current.node.syncManager.waitForAllCoValuesSync(3000);
      } catch {}

      if (navigation.to?.url) {
        isReplaying = true;
        try {
          const url = navigation.to.url;
          await props.navigation!.goto(
            url.pathname + url.search + url.hash,
          );
        } finally {
          isReplaying = false;
        }
      }
    });
  }

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
          onLogOut: props.onLogOut,
        })
        .catch((error) => {
          console.error("Error creating Jazz browser context:", error);
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
