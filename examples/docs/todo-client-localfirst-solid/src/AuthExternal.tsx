import { Show, type ParentProps } from "solid-js";
import { createAccountManager, type DbConfig } from "jazz-tools";
import { JazzProvider, createAccountState } from "jazz-tools/solid";

// Login starts outside a context. Linking requires the old context to complete
// shutdown({ waitForSync: true }) before calling accounts.linkJWT.
export function AuthExternal(
  props: ParentProps<{
    accounts: Awaited<ReturnType<typeof createAccountManager>>;
    getToken: () => Promise<string>;
    config: Omit<DbConfig, "account">;
  }>,
) {
  const state = createAccountState(props.accounts);
  return (
    <Show
      when={state().account}
      fallback={
        <>
          <button
            disabled={!!state().pending}
            onClick={() => {
              void props.accounts.loginJWT({ getToken: props.getToken }).catch(() => {});
            }}
          >
            Sign in
          </button>
          <Show when={state().error}>{(error) => <p role="alert">{error().message}</p>}</Show>
        </>
      }
    >
      {(account) => (
        <JazzProvider config={{ ...props.config, account: account() }}>
          {props.children}
        </JazzProvider>
      )}
    </Show>
  );
}
