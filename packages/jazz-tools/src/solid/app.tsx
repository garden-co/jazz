import {
  createContext,
  createEffect,
  createSignal,
  onCleanup,
  onMount,
  splitProps,
  Show,
  useContext,
  type Accessor,
  type JSX,
} from "solid-js";
import { createJazzApp, type JazzAppConfig } from "../session/create-jazz-app.js";
import type { JazzApp, JazzAppSnapshot } from "../session/app.js";
import type { JazzClient } from "../web/create-jazz-client.js";
import { ActiveClient } from "./session.js";
import { LegacyJazzProvider, type LegacyJazzProviderProps } from "./provider.js";

export interface UseJazzAuth {
  snapshot: Accessor<JazzAppSnapshot<JazzClient>>;
  logout(): Promise<void>;
  retry(): Promise<void>;
}
const AuthContext = createContext<UseJazzAuth>();
export function useJazzAuth(): UseJazzAuth {
  const auth = useContext(AuthContext);
  if (!auth) throw new Error("useJazzAuth must be used within JazzProvider");
  return auth;
}
export type JazzAppProviderProps = JazzAppConfig & {
  children: JSX.Element;
  signedOut?: JSX.Element;
  loading?: JSX.Element;
  error?: (error: Error, retry: () => Promise<void>) => JSX.Element;
  autoAttachDevTools?: boolean;
};
export type JazzProviderProps = JazzAppProviderProps | LegacyJazzProviderProps;
/** Own the common auth/session lifecycle; JSX getters preserve Solid reactivity. */
export function JazzProvider(props: JazzProviderProps) {
  if ("config" in props) return <LegacyJazzProvider {...props} />;
  return <AppProvider {...props} />;
}
function AppProvider(props: JazzAppProviderProps) {
  const [, config] = splitProps(props, [
    "children",
    "signedOut",
    "loading",
    "error",
    "autoAttachDevTools",
  ]);
  const [snapshot, setSnapshot] = createSignal<JazzAppSnapshot<JazzClient>>({ status: "starting" });
  let owner: JazzApp<JazzClient> | undefined;
  let acknowledge: (() => void) | undefined;
  const retry = () => owner?.retry() ?? Promise.resolve();
  const value: UseJazzAuth = {
    snapshot,
    retry,
    logout: () => owner?.logout() ?? Promise.resolve(),
  };
  onMount(() => {
    const app = createJazzApp(config);
    owner = app;
    const lease = app.attachConsumer();
    const update = () => setSnapshot(app.getSnapshot());
    const stop = app.subscribe(update);
    acknowledge = () => {
      const observed = snapshot();
      queueMicrotask(() => lease.acknowledge(observed));
    };
    update();
    onCleanup(() => {
      owner = undefined;
      stop();
      lease.release();
      void app.dispose().catch(console.error);
    });
  });
  createEffect(() => {
    owner?.updateAuth(props.auth);
  });
  createEffect(() => {
    snapshot();
    acknowledge?.();
  });
  const fallback = () => (
    <Show
      when={snapshot().status === "error" ? snapshot().error : undefined}
      keyed
      fallback={
        <Show
          when={snapshot().status === "signed-out"}
          fallback={props.loading ?? <p role="status">Loading...</p>}
        >
          {props.signedOut}
        </Show>
      }
    >
      {(error) =>
        props.error ? (
          props.error(error, retry)
        ) : (
          <>
            <p role="alert">{error.message}</p>
            <button onClick={() => void retry().catch(() => {})}>Retry</button>
          </>
        )
      }
    </Show>
  );
  return (
    <AuthContext.Provider value={value}>
      <Show when={snapshot().client} keyed fallback={fallback()}>
        {(client) => (
          <ActiveClient client={client} autoAttachDevTools={props.autoAttachDevTools}>
            {props.children}
          </ActiveClient>
        )}
      </Show>
    </AuthContext.Provider>
  );
}
