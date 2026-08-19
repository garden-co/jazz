import { Show } from "solid-js";
import { JazzProvider, createSolidJazzClient, useLocalFirstAuth } from "jazz-tools/solid";
import { TodoList } from "./TodoList.js";

export function App() {
  const auth = useLocalFirstAuth();

  return (
    <Show when={!auth.isLoading && auth.secret} fallback={<p>Loading...</p>}>
      {(secret) => {
        const client = createSolidJazzClient(() => ({
          appId: "<your-app-id>",
          secret: secret(),
        }));
        return (
          <JazzProvider client={client}>
            <h1>Todos</h1>
            <TodoList />
          </JazzProvider>
        );
      }}
    </Show>
  );
}
