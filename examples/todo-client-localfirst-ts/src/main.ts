import { createDb, type DbConfig, type Db } from "jazz-ts";
import { app, Todo } from "../schema/app.js";

function orderTodosWithDepth(todos: Todo[]): { todo: Todo; depth: number }[] {
  const todoIds = new Set(todos.map((todo) => todo.id));
  const childrenByParent = new Map<string, Todo[]>();
  const roots: Todo[] = [];

  for (const todo of todos) {
    const parentId = todo.parent;
    if (parentId && todoIds.has(parentId)) {
      const siblings = childrenByParent.get(parentId) ?? [];
      siblings.push(todo);
      childrenByParent.set(parentId, siblings);
    } else {
      roots.push(todo);
    }
  }

  const ordered: { todo: Todo; depth: number }[] = [];
  const visited = new Set<string>();

  const visit = (todo: Todo, depth: number) => {
    if (visited.has(todo.id)) return;
    visited.add(todo.id);
    ordered.push({ todo, depth });
    const children = childrenByParent.get(todo.id) ?? [];
    for (const child of children) {
      visit(child, depth + 1);
    }
  };

  for (const root of roots) {
    visit(root, 0);
  }

  // Handle cycles or disconnected nodes defensively.
  for (const todo of todos) {
    visit(todo, 0);
  }

  return ordered;
}

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
  const parentSelect = document.createElement("select");
  parentSelect.id = "parent-select";
  const noParentOption = document.createElement("option");
  noParentOption.value = "";
  noParentOption.textContent = "No parent";
  parentSelect.appendChild(noParentOption);
  form.appendChild(input);
  form.appendChild(parentSelect);
  form.appendChild(btn);
  container.appendChild(form);

  const selectedProjectId = db.insert(app.projects, { name: "Default Project" });

  const list = document.createElement("ul");
  list.id = "todo-list";
  container.appendChild(list);
  // Subscribe to the project & all its todos
  const query = app.todos.where({ project: selectedProjectId });
  db.subscribeAll(query, ({ all: todos }) => {
    const ordered = orderTodosWithDepth(todos);
    parentSelect.innerHTML = "";
    parentSelect.appendChild(noParentOption);
    for (const todo of todos) {
      const option = document.createElement("option");
      option.value = todo.id;
      option.textContent = todo.title;
      parentSelect.appendChild(option);
    }

    list.innerHTML = ordered
      .map(
        ({ todo, depth }) => `
      <li class="${todo.done ? "done" : ""}" data-depth="${depth}" style="padding-left: ${depth * 20}px;">
        <input type="checkbox" ${todo.done ? "checked" : ""}
               data-id="${todo.id}" class="toggle">
        <span>${todo.title}</span>
        ${todo.description ? `<small>${todo.description}</small>` : ""}
        <button data-id="${todo.id}" class="delete-btn">&times;</button>
      </li>
    `,
      )
      .join("");
  });

  // Add todo form
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const selectedParentId = parentSelect.value;
    db.insert(app.todos, {
      title: input.value,
      done: false,
      project: selectedProjectId,
      ...(selectedParentId ? { parent: selectedParentId } : {}),
    });
    input.value = "";
    parentSelect.value = "";
  });

  // Event delegation for toggle and delete
  list.addEventListener("click", async (e) => {
    const target = e.target as HTMLElement;
    const id = target.dataset.id;
    if (!id) return;

    if (target.classList.contains("toggle")) {
      const todo = await db.one(app.todos.where({ id }));
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
