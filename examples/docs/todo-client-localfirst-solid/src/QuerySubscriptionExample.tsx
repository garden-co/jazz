import { For } from "solid-js";
import { useAll } from "jazz-tools/solid";
import { app } from "../schema.js";

export function QuerySubscriptionExample() {
  const todos = useAll(() => ({ query: app.todos.where({ done: false }) }));
  return <For each={todos.data ?? []}>{(todo) => <li>{todo.title}</li>}</For>;
}
