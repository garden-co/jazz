import type { Db } from "jazz-tools";
import { sessionConfig, type SessionOverrides } from "./account.js";
import { createJazzClient, type JazzClient, type JazzSession } from "jazz-tools/client";
import { authSessionExamples } from "./auth-session-snippets.js";
import { app, type Todo } from "../schema.js";

// Keep docs-only auth snippets in the compiled example app.
void authSessionExamples;

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

function orderTodosWithDepth(todos: Todo[]): { todo: Todo; depth: number }[] {
  const todoIds = new Set(todos.map((todo) => todo.id));
  const childrenByParent = new Map<string, Todo[]>();
  const roots: Todo[] = [];

  for (const todo of todos) {
    const parentId = todo.parentId;
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

// #region context-setup-ts-client
async function openSession(config?: SessionOverrides) {
  const session = await createJazzClient(sessionConfig(config));
  // session.db is the signed-in user's Db; close the session on teardown.
  return session;
}
// #endregion context-setup-ts-client

// #region follow-session-ts
/**
 * Run `attach` on the session's current Db, and again each time the session
 * replaces it: when the user signs in, signs out or switches accounts. (The
 * upgrade to the local-first client keeps the same Db.)
 */
export function followDb(
  session: JazzSession<JazzClient>,
  attach: (db: Db) => () => void,
): () => void {
  let current: Db | undefined;
  let detach: (() => void) | undefined;
  const follow = () => {
    const next = session.getSnapshot().client?.db;
    if (next === current) return;
    detach?.();
    detach = undefined;
    current = next;
    if (next) detach = attach(next);
  };
  const stop = session.subscribe(follow);
  follow();
  return () => {
    stop();
    detach?.();
  };
}
// #endregion follow-session-ts

export async function startApp(
  container: HTMLElement,
  config?: SessionOverrides,
): Promise<{ session: JazzSession<JazzClient>; db: Db; destroy: () => Promise<void> }> {
  const session = await openSession(config);
  // Writes go to the current Db: the session swaps it when the user signs in or out.
  const currentDb = () => session.db;

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

  const list = document.createElement("ul");
  list.id = "todo-list";
  container.appendChild(list);
  // Subscribe to all todos.
  const query = app.todos;
  const render = (todos: Todo[]) => {
    const ordered = orderTodosWithDepth(todos);
    parentSelect.replaceChildren(noParentOption);
    for (const todo of todos) {
      const option = document.createElement("option");
      option.value = todo.id;
      option.textContent = todo.title;
      parentSelect.appendChild(option);
    }

    const items = document.createDocumentFragment();
    for (const { todo, depth } of ordered) {
      const item = document.createElement("li");
      item.classList.toggle("done", todo.done);
      item.dataset.depth = String(depth);
      item.style.paddingLeft = `${depth * 20}px`;

      const toggle = document.createElement("input");
      toggle.type = "checkbox";
      toggle.checked = todo.done;
      toggle.dataset.id = todo.id;
      toggle.className = "toggle";

      const title = document.createElement("span");
      title.textContent = todo.title;

      item.append(toggle, title);
      if (todo.description) {
        const description = document.createElement("small");
        description.textContent = todo.description;
        item.appendChild(description);
      }

      const deleteButton = document.createElement("button");
      deleteButton.dataset.id = todo.id;
      deleteButton.className = "delete-btn";
      deleteButton.textContent = "×";
      item.appendChild(deleteButton);
      items.appendChild(item);
    }
    list.replaceChildren(items);
  };
  const stopTodos = followDb(session, (db) => db.subscribe(query, render));

  // Add todo form
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const selectedParentId = parentSelect.value;
    currentDb().insert(app.todos, {
      title: input.value,
      done: false,
      ...(selectedParentId ? { parentId: selectedParentId } : {}),
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
      const checkbox = target as HTMLInputElement;
      currentDb().update(app.todos, id, { done: checkbox.checked });
    } else if (target.classList.contains("delete-btn")) {
      currentDb().delete(app.todos, id);
    }
  });

  return {
    session,
    get db() {
      return currentDb();
    },
    destroy: async () => {
      stopTodos();
      await session.close();
      container.innerHTML = "";
    },
  };
}
