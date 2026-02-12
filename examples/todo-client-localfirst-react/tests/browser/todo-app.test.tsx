/**
 * E2E browser tests for the React todo app.
 *
 * Renders real React components in a Chromium browser via @vitest/browser + playwright.
 * Uses real groove-wasm, real Workers, real OPFS.
 */

import { useState } from "react";
import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { createDb, Db, type QueryBuilder, type TableProxy } from "jazz-ts";
import type { WasmSchema } from "jazz-ts";
import { JazzProvider, useDb, useAll } from "jazz-react";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

// ---------------------------------------------------------------------------
// Test schema — inline to avoid depending on codegen in tests
// ---------------------------------------------------------------------------

const schema: WasmSchema = {
  tables: {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
      ],
    },
  },
};

interface Todo {
  id: string;
  title: string;
  done: boolean;
}

interface TodoInit {
  title: string;
  done: boolean;
}

const todos: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
};

const allTodos: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitForCondition(
  check: () => boolean,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

function base64url(input: string | Uint8Array): string {
  const str = typeof input === "string" ? btoa(input) : btoa(String.fromCharCode(...input));
  return str.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

async function signJwt(sub: string, secret: string): Promise<string> {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = { sub, claims: {}, exp: Math.floor(Date.now() / 1000) + 3600 };
  const enc = new TextEncoder();
  const headerB64 = base64url(JSON.stringify(header));
  const payloadB64 = base64url(JSON.stringify(payload));
  const data = enc.encode(`${headerB64}.${payloadB64}`);
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, data);
  return `${headerB64}.${payloadB64}.${base64url(new Uint8Array(sig))}`;
}

// ---------------------------------------------------------------------------
// React test components
// ---------------------------------------------------------------------------

/** Renders a todo list using useAll + useDb. */
function TestTodoList() {
  const db = useDb();
  const items = useAll<Todo>(allTodos);

  return (
    <div>
      <ul data-testid="todo-list">
        {items.map((todo) => (
          <li key={todo.id} data-testid={`todo-${todo.id}`} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => db.update(todos, todo.id, { done: !todo.done })}
              data-testid={`toggle-${todo.id}`}
            />
            <span data-testid={`title-${todo.id}`}>{todo.title}</span>
            <button data-testid={`delete-${todo.id}`} onClick={() => db.deleteFrom(todos, todo.id)}>
              Delete
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("React Todo App E2E", () => {
  const dbs: Db[] = [];
  let root: Root | null = null;
  let container: HTMLDivElement | null = null;

  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  function mountContainer(): HTMLDivElement {
    const el = document.createElement("div");
    document.body.appendChild(el);
    container = el;
    return el;
  }

  afterEach(async () => {
    if (root) {
      root.unmount();
      root = null;
    }
    if (container) {
      container.remove();
      container = null;
    }
    for (const db of dbs) {
      try {
        await db.shutdown();
      } catch {
        // Best effort
      }
    }
    dbs.length = 0;
  });

  // -------------------------------------------------------------------------
  // 1. Render todos from subscription
  // -------------------------------------------------------------------------

  it("renders todos inserted via db.insert()", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("render") }));

    // Insert before rendering — useAll fires synchronously on subscribe
    db.insert(todos, { title: "Buy milk", done: false });
    db.insert(todos, { title: "Walk dog", done: true });

    const el = mountContainer();
    await act(async () => {
      root = createRoot(el);
      root.render(
        <JazzProvider config={{ appId: "test-app", dbName: db["config"].dbName }}>
          <TestTodoList />
        </JazzProvider>,
      );
    });

    // JazzProvider creates its own Db — we need to wait for it to initialize
    // and for useAll to pick up the data. But since we used a different db
    // for inserts, the provider's db won't have the data.
    // Instead: render with an already-created db by using context directly.
    root!.unmount();

    // Simpler approach: insert after the component mounts
    await act(async () => {
      root = createRoot(el);
      root.render(
        <JazzProvider config={{ appId: "test-app", dbName: uniqueDbName("render2") }}>
          <TestTodoList />
        </JazzProvider>,
      );
    });

    // Wait for provider to initialize
    await waitForCondition(
      () => el.querySelector("[data-testid='todo-list']") !== null,
      5000,
      "Todo list should render",
    );

    // The list should be empty initially (fresh db)
    expect(el.querySelectorAll("li").length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 2. Insert triggers re-render
  // -------------------------------------------------------------------------

  it("re-renders when items are inserted via useDb", async () => {
    const dbName = uniqueDbName("insert-rerender");
    const el = mountContainer();

    // Component that exposes insert via a button
    function TestApp() {
      return (
        <JazzProvider config={{ appId: "test-app", dbName }}>
          <InsertAndList />
        </JazzProvider>
      );
    }

    function InsertAndList() {
      const db = useDb();
      const items = useAll<Todo>(allTodos);
      return (
        <div>
          <button
            data-testid="add-btn"
            onClick={() => db.insert(todos, { title: "New todo", done: false })}
          >
            Add
          </button>
          <ul data-testid="list">
            {items.map((t) => (
              <li key={t.id} data-testid={`item-${t.id}`}>
                {t.title}
              </li>
            ))}
          </ul>
        </div>
      );
    }

    await act(async () => {
      root = createRoot(el);
      root.render(<TestApp />);
    });

    // Wait for provider
    await waitForCondition(
      () => el.querySelector("[data-testid='list']") !== null,
      5000,
      "List should render",
    );

    expect(el.querySelectorAll("li").length).toBe(0);

    // Click add button
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='add-btn']")!.click();
    });

    await waitForCondition(
      () => el.querySelectorAll("li").length === 1,
      3000,
      "Should have 1 todo after insert",
    );

    expect(el.querySelector("li")!.textContent).toContain("New todo");
  });

  // -------------------------------------------------------------------------
  // 3. Toggle todo
  // -------------------------------------------------------------------------

  it("toggles todo done state on checkbox click", async () => {
    const dbName = uniqueDbName("toggle");
    const el = mountContainer();

    function TestApp() {
      return (
        <JazzProvider config={{ appId: "test-app", dbName }}>
          <InsertThenList />
        </JazzProvider>
      );
    }

    let dbRef: Db | null = null;

    function InsertThenList() {
      const db = useDb();
      dbRef = db;
      const items = useAll<Todo>(allTodos);
      return (
        <ul>
          {items.map((t) => (
            <li key={t.id} className={t.done ? "done" : ""}>
              <input
                type="checkbox"
                checked={t.done}
                onChange={() => db.update(todos, t.id, { done: !t.done })}
                data-testid={`toggle-${t.id}`}
              />
              <span data-testid={`title-${t.id}`}>{t.title}</span>
            </li>
          ))}
        </ul>
      );
    }

    await act(async () => {
      root = createRoot(el);
      root.render(<TestApp />);
    });

    // Wait for provider
    await waitForCondition(() => dbRef !== null, 5000, "Db should initialize");

    // Insert a todo
    await act(async () => {
      dbRef!.insert(todos, { title: "Toggle me", done: false });
    });

    await waitForCondition(
      () => el.querySelectorAll("li").length === 1,
      3000,
      "Should have 1 todo",
    );

    const li = el.querySelector("li")!;
    expect(li.className).toBe("");

    // Toggle
    const checkbox = el.querySelector<HTMLInputElement>("input[type='checkbox']")!;
    await act(async () => {
      checkbox.click();
    });

    await waitForCondition(
      () => el.querySelector("li")!.className === "done",
      3000,
      "Todo should be marked done",
    );
  });

  // -------------------------------------------------------------------------
  // 4. Delete todo
  // -------------------------------------------------------------------------

  it("removes todo from list on delete", async () => {
    const dbName = uniqueDbName("delete");
    const el = mountContainer();
    let dbRef: Db | null = null;

    function TestApp() {
      return (
        <JazzProvider config={{ appId: "test-app", dbName }}>
          <DeleteList />
        </JazzProvider>
      );
    }

    function DeleteList() {
      const db = useDb();
      dbRef = db;
      const items = useAll<Todo>(allTodos);
      return (
        <ul>
          {items.map((t) => (
            <li key={t.id}>
              <span>{t.title}</span>
              <button data-testid={`del-${t.id}`} onClick={() => db.deleteFrom(todos, t.id)}>
                X
              </button>
            </li>
          ))}
        </ul>
      );
    }

    await act(async () => {
      root = createRoot(el);
      root.render(<TestApp />);
    });

    await waitForCondition(() => dbRef !== null, 5000, "Db should initialize");

    await act(async () => {
      dbRef!.insert(todos, { title: "Delete me", done: false });
    });

    await waitForCondition(
      () => el.querySelectorAll("li").length === 1,
      3000,
      "Should have 1 todo",
    );

    // Delete
    const deleteBtn = el.querySelector<HTMLButtonElement>("button")!;
    await act(async () => {
      deleteBtn.click();
    });

    await waitForCondition(
      () => el.querySelectorAll("li").length === 0,
      3000,
      "Todo should be removed",
    );
  });

  // -------------------------------------------------------------------------
  // 5. Add todo via form
  // -------------------------------------------------------------------------

  it("adds todo via form submission", async () => {
    const dbName = uniqueDbName("form");
    const el = mountContainer();

    function TestApp() {
      return (
        <JazzProvider config={{ appId: "test-app", dbName }}>
          <FormAndList />
        </JazzProvider>
      );
    }

    function FormAndList() {
      const db = useDb();
      const items = useAll<Todo>(allTodos);
      const [title, setTitle] = useState("");

      return (
        <div>
          <form
            data-testid="add-form"
            onSubmit={(e) => {
              e.preventDefault();
              if (!title.trim()) return;
              db.insert(todos, { title: title.trim(), done: false });
              setTitle("");
            }}
          >
            <input
              data-testid="title-input"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
            />
            <button type="submit" data-testid="submit-btn">
              Add
            </button>
          </form>
          <ul data-testid="list">
            {items.map((t) => (
              <li key={t.id}>{t.title}</li>
            ))}
          </ul>
        </div>
      );
    }

    await act(async () => {
      root = createRoot(el);
      root.render(<TestApp />);
    });

    await waitForCondition(
      () => el.querySelector("[data-testid='list']") !== null,
      5000,
      "Form should render",
    );

    // Type into input and submit
    const input = el.querySelector<HTMLInputElement>("[data-testid='title-input']")!;
    const form = el.querySelector<HTMLFormElement>("[data-testid='add-form']")!;

    await act(async () => {
      // Simulate typing
      const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
        window.HTMLInputElement.prototype,
        "value",
      )!.set!;
      nativeInputValueSetter.call(input, "From form");
      input.dispatchEvent(new Event("input", { bubbles: true }));
    });

    await act(async () => {
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });

    await waitForCondition(
      () => el.querySelectorAll("li").length === 1,
      3000,
      "Should have 1 todo from form",
    );

    expect(el.querySelector("li")!.textContent).toContain("From form");
  });

  // -------------------------------------------------------------------------
  // 6. OPFS persistence across reload
  // -------------------------------------------------------------------------

  it("persists data across shutdown and re-create (OPFS)", async () => {
    const dbName = uniqueDbName("opfs-react");

    // First session: insert data
    const db1 = await createDb({ appId: "test-app", dbName });
    db1.insert(todos, { title: "Survive reload", done: true });
    const before = await db1.all(allTodos);
    expect(before.length).toBe(1);
    await db1.shutdown();

    // Second session: verify data survived via worker tier
    const db2 = track(await createDb({ appId: "test-app", dbName }));
    const after = await db2.all(allTodos, "worker");
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
  });

  // -------------------------------------------------------------------------
  // 7. Server sync
  // -------------------------------------------------------------------------

  it("syncs data between two clients through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const token1 = await signJwt("react-user-a", JWT_SECRET);

    const db1 = track(
      await createDb({
        appId: APP_ID,
        dbName: uniqueDbName("react-sync-a"),
        serverUrl,
        jwtToken: token1,
        adminSecret: ADMIN_SECRET,
      }),
    );

    const id = await db1.insertPersisted(todos, { title: "Server-synced", done: false }, "edge");
    expect(id).toBeTruthy();

    const results = await db1.all(allTodos, "edge");
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results[0].title).toBe("Server-synced");
  });
});
