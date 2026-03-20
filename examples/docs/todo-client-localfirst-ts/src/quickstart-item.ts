import type { Db } from "jazz-tools";
import type { Todo, GeneratedApp } from "../schema/app.js";

export function renderTodoItem(todo: Todo, db: Db, app: GeneratedApp) {
  const li = Object.assign(document.createElement("li"), {
    textContent: todo.title,
  });

  const toggle = Object.assign(document.createElement("input"), {
    type: "checkbox",
    checked: todo.done,
    onchange: () => db.update(app.todos, todo.id, { done: !todo.done }),
  });

  const remove = Object.assign(document.createElement("button"), {
    textContent: "\u00d7",
    onclick: () => db.delete(app.todos, todo.id),
  });

  li.prepend(toggle);
  li.append(remove);
  return li;
}
