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
  createMemo,
  createSignal,
  onCleanup,
} from "solid-js";
import {
  JazzContext,
  type JazzContextValue,
  JazzManagerContext,
} from "./jazz.js";

type JazzAccount = AccountClass<Account> & CoValueFromRaw<Account>;

export type JazzProviderProps<S extends JazzAccount | AnyAccountSchema> = {
  readonly enableSSR?: boolean;
} & JazzContextManagerProps<S>;

export const JazzProvider = <S extends JazzAccount | AnyAccountSchema>(
  props: ParentProps<JazzProviderProps<S>>,
) => {
  const jazzManager = createMemo(
    () =>
      new JazzBrowserContextManager<S>({
        useAnonymousFallback: props.enableSSR,
      }),
  );

  const [ctx, setCtx] = createSignal<JazzContextValue<InstanceOfSchema<S>>>();

  createEffect(() => {
    jazzManager()
      .createContext({
        sync: props.sync,
        storage: props.storage,
        guestMode: props.guestMode,
        AccountSchema: props.AccountSchema,
        defaultProfileName: props.defaultProfileName,
        onAnonymousAccountDiscarded: props.onAnonymousAccountDiscarded,
        onLogOut: props.onLogOut,
        logOutReplacement: props.logOutReplacement,
      })
      .catch((error) => {
        console.error("Error creating Jazz browser context:", error);
      });
  });

  const authStorage = () => jazzManager().getAuthSecretStorage();

  createEffect(() => {
    const unsubscribe = jazzManager().subscribe(() => {
      setCtx(jazzManager().getCurrentValue());
    });

    onCleanup(() => {
      unsubscribe();
    });
  });

  onCleanup(() => {
    jazzManager().done();
  });

  return (
    <Show when={ctx()}>
      {(ctxValue) => {
        return (
          <JazzContext.Provider value={ctxValue}>
            <JazzManagerContext.Provider value={jazzManager}>
              {props.children}
            </JazzManagerContext.Provider>
          </JazzContext.Provider>
        );
      }}
    </Show>
  );
};
