import {
  createDb,
  createSyntheticUserSwitcher,
  getActiveSyntheticAuth,
  type DbConfig,
  type Db,
} from "jazz-tools";
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
  const activeAuth = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  const resolvedConfig: DbConfig = {
    appId,
    env: "dev",
    userBranch: "main",
    localAuthMode: activeAuth.localAuthMode,
    localAuthToken: activeAuth.localAuthToken,
    ...config,
  };

  // #region context-setup-ts-client
  const db = await createDb(resolvedConfig);
  const session = db.getAuthState().session;
  // #endregion context-setup-ts-client
  const sessionUserId = session?.user_id ?? null;

  // Build DOM
  const authControls = document.createElement("div");
  authControls.id = "auth-controls";
  container.appendChild(authControls);

  const switcher = createSyntheticUserSwitcher({
    appId: resolvedConfig.appId,
    container: authControls,
    defaultMode: "demo",
  });

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
  btn.disabled = !sessionUserId;
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
  const unsubscribe = db.subscribeAll(query, ({ all: todos }) => {
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
        <input type="checkbox" ${todo.done ? "checked" : ""} data-id="${todo.id}" class="toggle">
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
      switcher.destroy();
      await db.shutdown();
      container.innerHTML = "";
    },
  };
}
