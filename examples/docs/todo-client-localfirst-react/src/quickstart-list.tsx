import { useAll } from "jazz-tools/react";
import { app } from "../jazz.generated.js";
import { TodoItem } from "./TodoItem.js";
import { AddTodo } from "./AddTodo.js";

export function TodoList() {
  const { data: todos = [] } = useAll(app.todos);

  return (
    <>
      <ul>
        {todos.map((todo) => (
          <TodoItem key={todo.id} id={todo.id} />
        ))}
      </ul>
      <AddTodo />
    </>
  );
}
