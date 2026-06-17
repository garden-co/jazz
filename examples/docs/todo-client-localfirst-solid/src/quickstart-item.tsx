import { Show } from "solid-js";
import { useAll, useDb } from "jazz-tools/solid";
import { app } from "../schema.js";

export function QuickstartItem(props: { id: string }) {
  const db = useDb();
  const todos = useAll(() => ({ query: app.todos.where({ id: props.id }).limit(1) }));
  const todo = () => todos.data?.[0];

  return (
    <Show when={todo()}>
      {(item) => (
        <li classList={{ done: item().done }}>
          <input
            type="checkbox"
            checked={item().done}
            onChange={() => db().update(app.todos, props.id, { done: !item().done })}
          />
          <span>{item().title}</span>
          <button onClick={() => db().delete(app.todos, props.id)}>&times;</button>
        </li>
      )}
    </Show>
  );
}
