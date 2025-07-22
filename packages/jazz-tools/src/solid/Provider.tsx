/**
 * @jsxImportSource solid-js
 */
import type { InstanceOfSchema } from "jazz-tools";
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
import {
  type ParentProps,
  Show,
  createEffect,
  createSignal,
  onCleanup,
  untrack,
} from "solid-js";
import { JazzAuthContext, JazzContext, JazzContextValue } from "./jazz.js";

type JazzAccount = AccountClass<Account> & CoValueFromRaw<Account>;

export const Provider = <S extends JazzAccount | AnyAccountSchema>(
  props: ParentProps<JazzContextManagerProps<S>>,
) => {
  const contextManager = new JazzBrowserContextManager<S>();
  const [ctx, setCtx] = createSignal<JazzContextValue<InstanceOfSchema<S>>>();

  // Effect to handle context creation
  createEffect(() => {
    untrack(() => {
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

  // Effect to subscribe to context manager updates
  createEffect(() => {
    const unsubscribe = contextManager.subscribe(() => {
      setCtx(contextManager.getCurrentValue());
    });

    onCleanup(() => {
      unsubscribe();
    });
  });

  return (
    <Show when={ctx()}>
      {(ctxValue) => {
        const authStorage = () => contextManager.getAuthSecretStorage();

        return (
          <JazzContext.Provider value={ctxValue}>
            <JazzAuthContext.Provider value={authStorage}>
              {props.children}
            </JazzAuthContext.Provider>
          </JazzContext.Provider>
        );
      }}
    </Show>
  );
};
