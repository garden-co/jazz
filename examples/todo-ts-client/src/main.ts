import { createDb, IndexedDBDriver } from "jazz-ts";
import { app, type Todo } from "../schema/app.js";

async function main() {
  // Open IndexedDB driver
  const driver = await IndexedDBDriver.open("todo-app");

  // Create Db (pre-loads WASM)
  const db = await createDb({
    appId: "todo-client-example",
    driver,
    env: "dev",
    userBranch: "main",
  });

  // Render function
  function render(todos: Todo[]) {
    const list = document.getElementById("todo-list")!;
    list.innerHTML = todos
      .map(
        (t) => `
      <li class="${t.done ? "done" : ""}">
        <input type="checkbox" ${t.done ? "checked" : ""}
               data-id="${t.id}" class="toggle">
        <span>${t.title}</span>
        ${t.description ? `<small>${t.description}</small>` : ""}
        <button data-id="${t.id}" class="delete-btn">&times;</button>
      </li>
    `,
      )
      .join("");
  }

  // Subscribe to all todos
  db.subscribeAll<Todo>(app.todos, ({ all }) => render(all));

  // Add todo form
  document.getElementById("add-form")!.addEventListener("submit", (e) => {
    e.preventDefault();
    const input = document.getElementById("title-input") as HTMLInputElement;
    db.insert(app.todos, {
      title: input.value,
      done: false,
    });
    input.value = "";
  });

  // Event delegation for toggle and delete
  document.getElementById("todo-list")!.addEventListener("click", async (e) => {
    const target = e.target as HTMLElement;
    const id = target.dataset.id;
    if (!id) return;

    if (target.classList.contains("toggle")) {
      const todo = await db.one<Todo>(app.todos.where({ id }));
      if (todo) {
        db.update(app.todos, id, { done: !todo.done });
      }
    } else if (target.classList.contains("delete-btn")) {
      db.deleteFrom(app.todos, id);
    }
  });
}

main().catch(console.error);
