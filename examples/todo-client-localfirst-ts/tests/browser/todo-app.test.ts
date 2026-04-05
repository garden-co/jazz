/**
 * E2E browser tests for the vanilla TS todo app.
 *
 * Calls startApp() to mount the real app into a container, then interacts
 * through the actual DOM it produces — form, checkboxes, delete buttons.
 */

import { describe, it, expect, afterEach } from "vitest";
import { startApp } from "../../src/main.js";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";
import { app } from "../../schema.js";
import { createDb, DbConfig } from "jazz-tools";

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

/** Submit a todo via the app's form. */
function addTodo(container: HTMLElement, title: string) {
  const input = container.querySelector<HTMLInputElement>("#title-input")!;
  const form = container.querySelector<HTMLFormElement>("#add-form")!;
  input.value = title;
  form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
}

/** Submit a child todo by selecting a parent title first. */
function addTodoWithParent(container: HTMLElement, title: string, parentTitle: string) {
  const parentSelect = container.querySelector<HTMLSelectElement>("#parent-select")!;
  const parentOption = [...parentSelect.options].find((opt) => opt.textContent === parentTitle);
  if (!parentOption?.value) {
    throw new Error(`Parent option "${parentTitle}" not found`);
  }
  parentSelect.value = parentOption.value;
  addTodo(container, title);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Vanilla TS Todo App E2E", () => {
  const instances: Array<{ container: HTMLDivElement; destroy: () => Promise<void> }> = [];

  /** Mount the app into a fresh container. */
  async function mount(config?: Partial<DbConfig>): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);

    const { destroy } = await startApp(el, config);
    instances.push({ container: el, destroy });

    // Wait for the app to render
    await waitFor(() => el.querySelector("#todo-list") !== null, 5000, "App should render");

    return el;
  }

  /** Destroy a specific instance. */
  async function destroyInstance(el: HTMLDivElement): Promise<void> {
    const idx = instances.findIndex((i) => i.container === el);
    if (idx === -1) return;
    await instances[idx].destroy();
    el.remove();
    instances.splice(idx, 1);
    // Give OPFS handles time to release
    await new Promise((r) => setTimeout(r, 200));
  }

  afterEach(async () => {
    for (const { container, destroy } of instances) {
      try {
        await destroy();
      } catch {
        /* best effort */
      }
      container.remove();
    }
    instances.length = 0;
  });

  // -------------------------------------------------------------------------
  // 1. App renders with empty list
  // -------------------------------------------------------------------------

  it("renders the app with an empty todo list", async () => {
    const el = await mount({ driver: { type: "persistent", dbName: uniqueDbName("empty") } });

    expect(el.querySelector("h1")!.textContent).toBe("Todos");
    expect(el.querySelector("#todo-list")).toBeTruthy();
    expect(el.querySelectorAll("#todo-list li").length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 2. Add todo via form
  // -------------------------------------------------------------------------

  it("adds a todo via the form", async () => {
    const el = await mount({ driver: { type: "persistent", dbName: uniqueDbName("add") } });

    addTodo(el, "Buy milk");

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear after form submit",
    );

    const li = el.querySelector("#todo-list li")!;
    expect(li.querySelector("span")!.textContent).toBe("Buy milk");
    expect(li.classList.contains("done")).toBe(false);
  });

  it("renders child todos directly under their parent with nesting depth", async () => {
    const el = await mount({
      driver: { type: "persistent", dbName: uniqueDbName("parent-child") },
    });

    addTodo(el, "Parent task");

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Parent todo should appear",
    );

    addTodoWithParent(el, "Child task", "Parent task");

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 2,
      3000,
      "Child todo should appear",
    );

    const items = [...el.querySelectorAll<HTMLLIElement>("#todo-list li")];
    const titles = items.map((li) => li.querySelector("span")!.textContent);
    expect(titles).toEqual(["Parent task", "Child task"]);
    expect(items[0].dataset.depth).toBe("0");
    expect(items[1].dataset.depth).toBe("1");
  });

  // -------------------------------------------------------------------------
  // 3. Toggle todo
  // -------------------------------------------------------------------------

  it("toggles a todo's done state via checkbox", async () => {
    const el = await mount({ driver: { type: "persistent", dbName: uniqueDbName("toggle") } });

    addTodo(el, "Toggle me");

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear",
    );

    const li = el.querySelector("#todo-list li")!;
    expect(li.classList.contains("done")).toBe(false);

    // Click the checkbox — toggle handler is async (db.one then db.update)
    const checkbox = li.querySelector<HTMLInputElement>("input.toggle")!;
    checkbox.dispatchEvent(new MouseEvent("click", { bubbles: true }));

    await waitFor(
      () => el.querySelector("#todo-list li")!.classList.contains("done"),
      5000,
      "Todo should be marked done",
    );
  });

  // -------------------------------------------------------------------------
  // 4. Delete todo
  // -------------------------------------------------------------------------

  it("deletes a todo via the delete button", async () => {
    const el = await mount({ driver: { type: "persistent", dbName: uniqueDbName("delete") } });

    addTodo(el, "Delete me");

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear",
    );

    const deleteBtn = el.querySelector<HTMLButtonElement>(".delete-btn")!;
    deleteBtn.click();

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 0,
      3000,
      "Todo should be removed",
    );
  });

  // -------------------------------------------------------------------------
  // 5. Multiple todos
  // -------------------------------------------------------------------------

  it("renders multiple todos", async () => {
    const el = await mount({ driver: { type: "persistent", dbName: uniqueDbName("multi") } });

    addTodo(el, "First");
    addTodo(el, "Second");
    addTodo(el, "Third");

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

  it("persists todos across app destroy and remount (OPFS)", async () => {
    const dbName = uniqueDbName("opfs");

    // First session: mount, add todo, destroy
    const el1 = await mount({ driver: { type: "persistent", dbName } });
    addTodo(el1, "Survive reload");

    await waitFor(
      () => el1.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear in first session",
    );

    await destroyInstance(el1);

    // Second session: remount with same dbName — OPFS data should load
    const el2 = await mount({ driver: { type: "persistent", dbName } });

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      5000,
      "Todo should survive remount from OPFS",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Survive reload");
  });

  // -------------------------------------------------------------------------
  // 7. Server sync between two app instances
  // -------------------------------------------------------------------------

  it("syncs a todo between two app instances through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    const el1 = await mount({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-a") },
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: "ts-sync-user-a",
      adminSecret: ADMIN_SECRET,
    });
    const el2 = await mount({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-b") },
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: "ts-sync-user-b",
      adminSecret: ADMIN_SECRET,
    });

    // Let both app instances finish server/event-stream setup before mutating.
    await new Promise((r) => setTimeout(r, 750));

    // Add a todo in app 1
    addTodo(el1, "Synced todo");

    await waitFor(
      () => el1.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear in app 1",
    );

    // Wait for it to appear in app 2 via server sync
    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      10000,
      "Todo should sync to app 2 through the server",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Synced todo");
  });
});
