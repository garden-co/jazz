/** @jsxImportSource solid-js */
import { Account, AnonymousJazzAgent } from "jazz-tools";
import { TestJazzContextManager } from "jazz-tools/testing";
import {
  ParentProps,
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
} from "./context/jazz.js";

type JazzTestProviderProps<Acc extends Account> = {
  readonly account?: Acc | { guest: AnonymousJazzAgent };
  readonly isAuthenticated?: boolean;
};

export function JazzTestProvider<Acc extends Account>(
  props: ParentProps<JazzTestProviderProps<Acc>>,
) {
  const [ctx, setCtx] = createSignal<JazzContextValue<Acc>>();

  const jazzManager = createMemo(() => {
    const account = props.account ?? (Account.getMe() as Acc);

    return TestJazzContextManager.fromAccountOrGuest<Acc>(account, {
      isAuthenticated: props.isAuthenticated,
    });
  });

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
      {(jazz) => (
        <JazzContext.Provider value={jazz}>
          <JazzManagerContext.Provider value={jazzManager}>
            {props.children}
          </JazzManagerContext.Provider>
        </JazzContext.Provider>
      )}
    </Show>
  );
}

export {
  createJazzTestAccount,
  createJazzTestGuest,
  linkAccounts,
  setActiveAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
