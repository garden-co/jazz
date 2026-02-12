import { createDb, type DbConfig, type Db } from "jazz-ts";
import { app, type Todo } from "../schema/app.js";

export async function startApp(
  container: HTMLElement,
  config?: Partial<DbConfig>,
): Promise<{ db: Db; destroy: () => Promise<void> }> {
  const db = await createDb({
    appId: "todo-client-example",
    env: "dev",
    userBranch: "main",
    ...config,
  });

  // Build DOM
  const h1 = document.createElement("h1");
  h1.textContent = "Todos";
  container.appendChild(h1);

  const form = document.createElement("form");
  form.id = "add-form";
  const input = document.createElement("input");
  input.type = "text";
  input.id = "title-input";
  input.placeholder = "What needs to be done?";
  input.required = true;
  const btn = document.createElement("button");
  btn.type = "submit";
  btn.textContent = "Add";
  form.appendChild(input);
  form.appendChild(btn);
  container.appendChild(form);

  const list = document.createElement("ul");
  list.id = "todo-list";
  container.appendChild(list);

  // Render function
  function render(todos: Todo[]) {
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
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    db.insert(app.todos, {
      title: input.value,
      done: false,
    });
    input.value = "";
  });

  // Event delegation for toggle and delete
  list.addEventListener("click", async (e) => {
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

  return {
    db,
    destroy: async () => {
      await db.shutdown();
      container.innerHTML = "";
    },
  };
}
