import { Show, createResource, type JSX } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "solid-sonner";
import { TodoList } from "./TodoList.js";
import { prepareAccountConfig } from "./account.js";

type AppProps = { config?: Partial<DbConfig>; fallback?: JSX.Element };
const defaults = {};
export function App(props: AppProps = {}) {
  const [config] = createResource(() => props.config ?? defaults, prepareAccountConfig);
  return (
    <Show when={config()} keyed fallback={props.fallback ?? <p>Loading...</p>}>
      {(ready) => (
        <JazzProvider config={ready} fallback={props.fallback ?? <p>Loading...</p>}>
          <h1>Todos</h1>
          <TodoList />
          <Toaster />
        </JazzProvider>
      )}
    </Show>
  );
}
