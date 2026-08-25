import { createDb, BrowserAuthSecretStore, type DbConfig, type Db } from "jazz-tools";
import { app, type Todo } from "../schema.js";

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

export async function startApp(
  container: HTMLElement,
  config?: Partial<DbConfig>,
): Promise<{ db: Db; destroy: () => Promise<void> }> {
  const appId = config?.appId ?? readEnvAppId() ?? "019d4349-241f-71c6-a453-e4754063b3dc";

  const secret = config?.secret ?? (await BrowserAuthSecretStore.getOrCreateSecret({ appId }));

  const resolvedConfig: DbConfig = {
    appId,
    env: "dev",
    secret,
    ...config,
  };

  // #region context-setup-ts-client
  const db = await createDb(resolvedConfig);
  // #endregion context-setup-ts-client
  let sessionUserId = db.getAuthState().session?.user_id ?? null;

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
  const syncAuthState = (userId: string | null) => {
    sessionUserId = userId;
    btn.disabled = !sessionUserId;
  };
  syncAuthState(sessionUserId);
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

  const errorMessage = document.createElement("p");
  errorMessage.id = "error-message";
  errorMessage.hidden = true;
  errorMessage.setAttribute("role", "alert");
  container.appendChild(errorMessage);

  const list = document.createElement("ul");
  list.id = "todo-list";
  container.appendChild(list);

  const setErrorMessage = (message: string) => {
    errorMessage.textContent = message;
    errorMessage.hidden = false;
  };

  const clearErrorMessage = () => {
    errorMessage.textContent = "";
    errorMessage.hidden = true;
  };

  // Subscribe to all todos.
  const query = app.todos;
  const unsubscribe = db.subscribe(query, (todos) => {
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
  });
  const stopAuthSync = db.onAuthChanged(({ session }) => {
    syncAuthState(session?.user_id ?? null);
  });

  // Add todo form
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    if (!sessionUserId) return;
    clearErrorMessage();
    const selectedParentId = parentSelect.value;
    db.insert(app.todos, {
      title: input.value,
      done: false,
      owner_id: sessionUserId,
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
      try {
        db.update(app.todos, id, { done: checkbox.checked });
        clearErrorMessage();
      } catch {
        checkbox.checked = !checkbox.checked;
        setErrorMessage("You don't have permission to update this task");
      }
    } else if (target.classList.contains("delete-btn")) {
      try {
        db.delete(app.todos, id);
        clearErrorMessage();
      } catch {
        setErrorMessage("You don't have permission to delete this task");
      }
    }
  });

  return {
    db,
    destroy: async () => {
      unsubscribe();
      stopAuthSync();
      await db.shutdown();
      container.innerHTML = "";
    },
  };
}
