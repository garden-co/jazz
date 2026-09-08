import { type JSX } from "solid-js";
import { JazzProvider, type JazzAppConfig } from "jazz-tools/solid";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "solid-sonner";
import { TodoList } from "./TodoList.js";

type AppProps = { config?: Partial<DbConfig>; fallback?: JSX.Element };
export function App(props: AppProps = {}) {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
  const config = (): JazzAppConfig => ({
    appId: env?.VITE_JAZZ_APP_ID ?? env?.JAZZ_APP_ID ?? "",
    serverUrl: env?.VITE_JAZZ_SERVER_URL ?? env?.JAZZ_SERVER_URL,
    env: "dev",
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
