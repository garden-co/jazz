import { For, Show, createMemo, createSignal } from "solid-js";
import { useAll } from "jazz-tools/solid";
import { app } from "../schema.js";

export function ConditionalQueryExample() {
  const [filter, setFilter] = createSignal<string | null>(null);
  const query = createMemo(() =>
    filter() ? app.todos.where({ title: { contains: filter()! } }) : undefined,
  );
  const filtered = useAll(() => ({ query: query() }));

  return (
    <>
      <input
        value={filter() ?? ""}
        onInput={(e) => setFilter(e.currentTarget.value || null)}
        placeholder="Filter by title"
      />
      <Show when={filtered.data}>
        <ul>
          <For each={filtered.data ?? []}>{(todo) => <li>{todo.title}</li>}</For>
        </ul>
      </Show>
    </>
  );
}
