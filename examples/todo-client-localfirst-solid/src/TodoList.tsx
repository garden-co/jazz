import { For, createMemo, createSignal, type JSX } from "solid-js";
import { useAll, useDb, useSession } from "jazz-tools/solid";
import { toast } from "solid-sonner";
import { app, type Todo } from "./lib/schema.js";

export function TodoList() {
  const db = useDb();
  const session = useSession();
  const [title, setTitle] = createSignal("");

  const sessionUserId = createMemo(() => session()?.user_id ?? null);
  const todos = useAll(() => ({ query: app.todos }));

  const handleSubmit: JSX.EventHandler<HTMLFormElement, SubmitEvent> = (e) => {
    e.preventDefault();

    const trimmedTitle = title().trim();
    const ownerId = sessionUserId();
    if (!trimmedTitle || !ownerId) return;

    db().insert(app.todos, {
      title: trimmedTitle,
      done: false,
      owner_id: ownerId,
    });

    setTitle("");
  };

  const toggleTodo = (todo: Todo, event: Event) => {
    const checkbox = event.currentTarget as HTMLInputElement;

    try {
      db().update(app.todos, todo.id, { done: !todo.done });
    } catch {
      checkbox.checked = todo.done;
      toast.error("You don't have permission to update this task");
    }
  };

  const deleteTodo = (todoId: string) => {
    try {
      db().delete(app.todos, todoId);
    } catch {
      toast.error("You don't have permission to delete this task");
    }
  };

  return (
    <>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          value={title()}
          onInput={(e) => setTitle(e.currentTarget.value)}
          placeholder="What needs to be done?"
          required
        />
        <button type="submit" disabled={!sessionUserId()}>
          Add
        </button>
      </form>
      <ul id="todo-list">
        <For each={todos.data ?? []}>
          {(todo) => (
            <li classList={{ done: todo.done }}>
              <input
                type="checkbox"
                checked={todo.done}
                onChange={(event) => toggleTodo(todo, event)}
                class="toggle"
              />
              <span>{todo.title}</span>
              {todo.description ? <small>{todo.description}</small> : null}
              <button class="delete-btn" onClick={() => deleteTodo(todo.id)}>
                &times;
              </button>
            </li>
          )}
        </For>
      </ul>
    </>
  );
}
