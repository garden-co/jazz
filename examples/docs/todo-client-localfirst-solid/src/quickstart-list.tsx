import { For } from "solid-js";
import { useAll } from "jazz-tools/solid";
import { app } from "../schema.js";
import { QuickstartItem } from "./quickstart-item.js";
import { QuickstartAdd } from "./quickstart-add.js";

export function QuickstartList() {
  const todos = useAll(() => ({ query: app.todos }));

  return (
    <>
      <ul>
        <For each={todos.data ?? []}>{(todo) => <QuickstartItem id={todo.id} />}</For>
      </ul>
      <QuickstartAdd />
    </>
  );
}
