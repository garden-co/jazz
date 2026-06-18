import { createSignal } from "solid-js";
import { useDb } from "jazz-tools/solid";
import { app } from "../schema.js";

export function QuickstartAdd() {
  const db = useDb();
  const [title, setTitle] = createSignal("");

  function addTodo() {
    db().insert(app.todos, { title: title(), done: false });
    setTitle("");
  }

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        addTodo();
      }}
    >
      <input
        value={title()}
        onInput={(e) => setTitle(e.currentTarget.value)}
        type="text"
        placeholder="What needs to be done?"
      />
      <button type="submit">Add</button>
    </form>
  );
}
