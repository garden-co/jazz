import { useState } from "react";
import { useDb } from "jazz-tools/react";
import { app } from "../schema/app.js";

export function AddTodo() {
  const db = useDb();
  const [title, setTitle] = useState("");

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        db.insert(app.todos, { title, done: false });
        setTitle("");
      }}
    >
      <input
        type="text"
        value={title}
        onChange={(e) => setTitle(e.target.value)}
        placeholder="What needs to be done?"
      />
      <button type="submit">Add</button>
    </form>
  );
}
