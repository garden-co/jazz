import { useDb, useAll } from "jazz-tools/react";
import { app } from "../schema/app.js";

export function TodoItem({ id }: { id: string }) {
  const db = useDb();
  const [todo] = useAll(app.todos.where({ id }).limit(1)) ?? [];

  if (!todo) return null;

  return (
    <li className={todo.done ? "done" : ""}>
      <input
        type="checkbox"
        checked={todo.done}
        onChange={() => db.update(app.todos, id, { done: !todo.done })}
      />
      <span>{todo.title}</span>
      <button onClick={() => db.delete(app.todos, id)}>&times;</button>
    </li>
  );
}
