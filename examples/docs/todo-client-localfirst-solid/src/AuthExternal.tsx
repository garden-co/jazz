import { Show, type ParentProps } from "solid-js";
import { JazzProvider, useJazzSession, type JazzClient, type JazzSession } from "jazz-tools/solid";

// A session you opened with await createJazzClient({ serverUrl: "/jazz" }).
export function AuthExternal(props: ParentProps<{ session: JazzSession<JazzClient> }>) {
  const { snapshot, refresh } = useJazzSession(props.session);
  return (
    <JazzProvider
      session={props.session}
      signedOut={
        <>
          {/* After your own sign-in has set its session cookie, ask your backend who is signed in. */}
          <button
            disabled={!!snapshot().pending}
            onClick={() => {
              void refresh().catch(() => {});
            }}
          >
            Sign in
          </button>
          <Show when={snapshot().error}>{(error) => <p role="alert">{error().message}</p>}</Show>
        </>
      }
    >
      {props.children}
    </JazzProvider>
  );
}
