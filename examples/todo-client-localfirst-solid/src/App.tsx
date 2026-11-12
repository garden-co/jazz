import { type JSX } from "solid-js";
import { JazzProvider, type JazzClientConfig } from "jazz-tools/solid";
import { Toaster } from "solid-sonner";
import { TodoList } from "./TodoList.js";

/** Tests may name an app id (taken as the client's `scope`) and pass session options. */
export type AppConfig = Omit<Partial<JazzClientConfig>, "appId"> & { appId?: string };

type AppProps = { config?: AppConfig; fallback?: JSX.Element };
export function App(props: AppProps = {}) {
  // The app's backend serves Jazz on this origin (`/jazz`), so the client needs no URL.
  // Tests point the app at a sync server by its app id; a browser client
  // takes that as its `scope` (it names no app of its own).
  const config = (): JazzClientConfig => {
    const { appId, ...rest } = props.config ?? {};
    return {
      env: "dev",
      ...rest,
      ...(appId === undefined ? {} : { scope: appId }),
      initial: "device",
    };
  };
  return (
    <JazzProvider config={config()} loading={props.fallback}>
      <h1>Todos</h1>
      <TodoList />
      <Toaster />
    </JazzProvider>
  );
}
