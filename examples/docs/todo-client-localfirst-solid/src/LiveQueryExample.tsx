import { For, Show } from "solid-js";
import { useAll } from "jazz-tools/solid";
import { app } from "../schema.js";

export function LiveQueryExample() {
  const todos = useAll(() => ({ query: app.todos }));
  return (
    <Show when={todos.data !== undefined} fallback={<p>Connecting...</p>}>
      <ul>
        <For each={todos.data ?? []}>{(todo) => <li>{todo.title}</li>}</For>
      </ul>
    </Show>
  );
}
