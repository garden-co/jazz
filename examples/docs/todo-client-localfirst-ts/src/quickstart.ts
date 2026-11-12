// #region setup-ts
import { createJazzClient } from "jazz-tools/client";
import { app } from "../schema.js";
import { renderTodoItem } from "./TodoItem.js";

// Your backend serves Jazz at /jazz (createJazz); the browser names no app.
// `initial: "device"` signs in as a device account: a guest that needs no sign-in.
const session = await createJazzClient({ serverUrl: "/jazz", initial: "device" });
const db = session.db;
// use session.close() to clean up when finished
// #endregion setup-ts

// #region list-ts
const list = document.getElementById("todos")!;

db.subscribe(app.todos, (todos) => {
  list.replaceChildren(...todos.map((todo) => renderTodoItem(todo, db, app)));
});
// #endregion list-ts

// #region add-ts
const form = document.createElement("form");
const input = Object.assign(document.createElement("input"), {
  placeholder: "What needs to be done?",
});
form.append(input, Object.assign(document.createElement("button"), { textContent: "Add" }));
form.onsubmit = (e) => {
  e.preventDefault();
  db.insert(app.todos, { title: input.value, done: false });
  input.value = "";
};
document.body.append(form);
// #endregion add-ts
