// #region setup-ts
import { createDb } from "jazz-tools";
import { app } from "../schema.js";
import { renderTodoItem } from "./TodoItem.js";

const db = await createDb({
  appId: "my-todo-app",
});
// #endregion setup-ts

// #region list-ts
const list = document.getElementById("todos")!;

db.subscribeAll(app.todos, ({ all: todos }) => {
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
