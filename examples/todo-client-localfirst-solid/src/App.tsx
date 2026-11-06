import { type JSX } from "solid-js";
import { JazzProvider, type JazzAppConfig } from "jazz-tools/solid";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "solid-sonner";
import { TodoList } from "./TodoList.js";

type AppProps = { config?: Partial<DbConfig>; fallback?: JSX.Element };
export function App(props: AppProps = {}) {
  // The app's backend serves Jazz on this origin (`/jazz`), so the client needs no URL.
  const config = (): JazzAppConfig => ({
    env: "dev",
    initial: "local-first",
    ...props.config,
  });
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
