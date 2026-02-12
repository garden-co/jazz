/**
 * E2E browser tests for the React todo app.
 *
 * Mounts the real <App /> component in a Chromium browser via @vitest/browser + playwright.
 * Interacts through the actual DOM the app renders — no test-only components.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { createDb, type QueryBuilder, type TableProxy } from "jazz-ts";
import type { WasmSchema } from "jazz-ts";
import { App } from "../../src/App.js";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

// ---------------------------------------------------------------------------
// Schema helpers — for OPFS/server tests that operate outside React
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

const todos: TableProxy<Todo, TodoInit> = { _table: "todos", _schema: schema };

const allTodos: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _build: () => JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] }),
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
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

/** Type into an input controlled by React (triggers React's onChange). */
function typeInto(input: HTMLInputElement, value: string) {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!;
  setter.call(input, value);
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("React Todo App E2E", () => {
  let root: Root | null = null;
  let container: HTMLDivElement | null = null;

  /** Mount the real App with a unique dbName. Returns the container element. */
  async function mountApp(dbName: string): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    container = el;

    await act(async () => {
      root = createRoot(el);
      root.render(<App config={{ appId: "test-app", dbName }} />);
    });

    // Wait for JazzProvider to initialize and TodoList to render
    await waitFor(
      () => el.querySelector("#todo-list") !== null,
      5000,
      "App should render the todo list",
    );

    return el;
  }

  afterEach(async () => {
    if (root) {
      // Unmounting triggers JazzProvider cleanup (db.shutdown)
      await act(async () => root!.unmount());
      root = null;
    }
    if (container) {
      container.remove();
      container = null;
    }
  });

  // -------------------------------------------------------------------------
  // 1. App renders with empty list
  // -------------------------------------------------------------------------

  it("renders the app with an empty todo list", async () => {
    const el = await mountApp(uniqueDbName("empty"));

    expect(el.querySelector("h1")!.textContent).toBe("Todos");
    expect(el.querySelector("#todo-list")).toBeTruthy();
    expect(el.querySelectorAll("#todo-list li").length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 2. Add todo via form
  // -------------------------------------------------------------------------

  it("adds a todo via the form", async () => {
    const el = await mountApp(uniqueDbName("add"));

    const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
    const form = input.closest("form")!;

    await act(async () => {
      typeInto(input, "Buy milk");
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear after form submit",
    );

    const li = el.querySelector("#todo-list li")!;
    expect(li.querySelector("span")!.textContent).toBe("Buy milk");
    expect(li.classList.contains("done")).toBe(false);
  });

  // -------------------------------------------------------------------------
  // 3. Toggle todo
  // -------------------------------------------------------------------------

  it("toggles a todo's done state via checkbox", async () => {
    const el = await mountApp(uniqueDbName("toggle"));

    // Add a todo first
    const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
    const form = input.closest("form")!;
    await act(async () => {
      typeInto(input, "Toggle me");
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear",
    );

    const li = el.querySelector("#todo-list li")!;
    expect(li.classList.contains("done")).toBe(false);

    // Click the checkbox
    const checkbox = li.querySelector<HTMLInputElement>("input.toggle")!;
    await act(async () => checkbox.click());

    await waitFor(
      () => el.querySelector("#todo-list li")!.classList.contains("done"),
      3000,
      "Todo should be marked done",
    );
  });

  // -------------------------------------------------------------------------
  // 4. Delete todo
  // -------------------------------------------------------------------------

  it("deletes a todo via the delete button", async () => {
    const el = await mountApp(uniqueDbName("delete"));

    // Add a todo
    const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
    const form = input.closest("form")!;
    await act(async () => {
      typeInto(input, "Delete me");
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear",
    );

    // Click the delete button
    const deleteBtn = el.querySelector<HTMLButtonElement>(".delete-btn")!;
    await act(async () => deleteBtn.click());

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 0,
      3000,
      "Todo should be removed",
    );
  });

  // -------------------------------------------------------------------------
  // 5. Multiple todos render correctly
  // -------------------------------------------------------------------------

  it("renders multiple todos with correct state", async () => {
    const el = await mountApp(uniqueDbName("multi"));

    const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
    const form = input.closest("form")!;

    // Add three todos
    for (const title of ["First", "Second", "Third"]) {
      await act(async () => {
        typeInto(input, title);
        form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      });
    }

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 3,
      3000,
      "Should have 3 todos",
    );

    const titles = [...el.querySelectorAll("#todo-list li span")].map((s) => s.textContent);
    expect(titles.sort()).toEqual(["First", "Second", "Third"]);
  });

  // -------------------------------------------------------------------------
  // 6. OPFS persistence across reload
  // -------------------------------------------------------------------------

  it("persists data across shutdown and re-create (OPFS)", async () => {
    const dbName = uniqueDbName("opfs");

    // First session: insert via raw Db
    const db1 = await createDb({ appId: "test-app", dbName });
    db1.insert(todos, { title: "Survive reload", done: true });
    expect((await db1.all(allTodos)).length).toBe(1);
    await db1.shutdown();

    // Second session: mount the real app with same dbName, query at worker tier
    const db2 = await createDb({ appId: "test-app", dbName });
    const after = await db2.all(allTodos, "worker");
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
    await db2.shutdown();
  });

  // -------------------------------------------------------------------------
  // 7. Server sync
  // -------------------------------------------------------------------------

  it("syncs data through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const token = await signJwt("react-user-a", JWT_SECRET);

    const db1 = await createDb({
      appId: APP_ID,
      dbName: uniqueDbName("sync"),
      serverUrl,
      jwtToken: token,
      adminSecret: ADMIN_SECRET,
    });

    const id = await db1.insertPersisted(todos, { title: "Server-synced", done: false }, "edge");
    expect(id).toBeTruthy();

    const results = await db1.all(allTodos, "edge");
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results[0].title).toBe("Server-synced");
    await db1.shutdown();
  });
});
