import { For, createSignal } from "solid-js";
import { useAll, useDb } from "jazz-tools/solid";
import { app } from "../schema.js";

export function TodoList() {
  const db = useDb();
  const todos = useAll(() => ({ query: app.todos }));
  const incompleteTodos = useAll(() => ({
    query: app.todos.where({ done: false }).orderBy("title", "asc").limit(50),
  }));
  const [title, setTitle] = createSignal("");

  function addTodo(todoTitle: string) {
    db().insert(app.todos, { title: todoTitle, done: false });
  }

  function toggleTodo(todo: { id: string; done: boolean }) {
    db().update(app.todos, todo.id, { done: !todo.done });
  }

  function removeTodo(id: string) {
    db().delete(app.todos, id);
  }

  async function addImportantTodo(todoTitle: string) {
    const { id } = await db()
      .insert(app.todos, { title: todoTitle, done: false })
      .wait({ tier: "edge" });
    await db().update(app.todos, id, { done: true }).wait({ tier: "edge" });
    await db().delete(app.todos, id).wait({ tier: "global" });
  }

  function handleSubmit(event: SubmitEvent) {
    event.preventDefault();
    if (!title().trim()) return;
    addTodo(title().trim());
    setTitle("");
  }

  void incompleteTodos;
  void addImportantTodo;

  return (
    <>
      <form onSubmit={handleSubmit}>
        <input
          value={title()}
          onInput={(e) => setTitle(e.currentTarget.value)}
          type="text"
          placeholder="What needs to be done?"
          required
        />
        <button type="submit">Add</button>
      </form>
      <ul id="todo-list">
        <For each={todos.data ?? []}>
          {(todo) => (
            <li classList={{ done: todo.done }}>
              <input
                type="checkbox"
                checked={todo.done}
                class="toggle"
                onChange={() => toggleTodo(todo)}
              />
              <span>{todo.title}</span>
              {todo.description ? <small>{todo.description}</small> : null}
              <button class="delete-btn" type="button" onClick={() => removeTodo(todo.id)}>
                &times;
              </button>
            </li>
          )}
        </For>
      </ul>
    </>
  );
}
