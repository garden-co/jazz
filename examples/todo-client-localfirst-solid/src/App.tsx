import { type JSX } from "solid-js";
import { JazzProvider, type JazzAppConfig } from "jazz-tools/solid";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "solid-sonner";
import { TodoList } from "./TodoList.js";

type AppProps = { config?: Partial<DbConfig>; fallback?: JSX.Element };
export function App(props: AppProps = {}) {
  // The app's backend serves Jazz on this origin (`/jazz`), so the client needs no URL.
  // Tests point the app at a sync server by its app id; a browser client
  // takes that as its `scope` (it names no app of its own).
  const config = (): JazzAppConfig => {
    const { appId, ...rest } = props.config ?? {};
    return {
      env: "dev",
      initial: "local-first",
      ...(rest as Partial<JazzAppConfig>),
      ...(appId === undefined ? {} : { scope: appId }),
    };
  };
  const content = () => (
    <>
      <h1>Todos</h1>
      <TodoList />
      <Toaster />
    </>
  );
  // Explicit caller-owned accounts remain useful to embedded apps and test fixtures.
  if (props.config?.account)
    return (
      <JazzProvider config={props.config as DbConfig} fallback={props.fallback}>
        {content()}
      </JazzProvider>
    );
  return (
    <JazzProvider {...config()} loading={props.fallback}>
      {content()}
    </JazzProvider>
  );
}
