import { Show } from "solid-js";
import { useAll } from "jazz-tools/solid";
import { app } from "../schema.js";

export function QuickstartItem(props: { id: string }) {
  const todos = useAll(() => ({ query: app.todos.where({ id: props.id }).limit(1) }));
  const todo = () => todos.data?.[0];

  return (
    <Show when={todo()}>
      {(item) => (
        <li classList={{ done: item().done }}>
          <input
            type="checkbox"
            checked={item().done}
            onChange={() => todos.db.update(app.todos, props.id, { done: !item().done })}
          />
          <span>{item().title}</span>
          <button onClick={() => todos.db.delete(app.todos, props.id)}>&times;</button>
        </li>
      )}
    </Show>
  );
}
