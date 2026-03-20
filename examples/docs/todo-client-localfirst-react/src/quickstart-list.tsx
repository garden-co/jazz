import { useAll } from "jazz-tools/react";
import { app } from "../schema/app.js";
import { TodoItem } from "./TodoItem.js";
import { AddTodo } from "./AddTodo.js";

export function TodoList() {
  const todos = useAll(app.todos) ?? [];

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
