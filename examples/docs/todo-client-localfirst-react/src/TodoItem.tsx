import { useAll } from "jazz-tools/react";
import { app } from "../schema.js";

export function TodoItem({ id }: { id: string }) {
  const { data: todos = [], db } = useAll(app.todos.where({ id }).limit(1));
  const [todo] = todos;

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
