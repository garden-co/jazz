import { todoCrudTest } from "../../../testing/todo-app-scenarios.js";
import { memoryAccountStore } from "../../../testing/accounts.js";
/**
 * E2E browser tests for the vanilla TS todo app.
 *
 * Calls startApp() to mount the real app into a container, then interacts
 * through the actual DOM it produces — form, checkboxes, delete buttons.
 */

import { describe, it, expect, afterEach } from "vitest";
import { startApp } from "../../src/main.js";
import { app } from "../../schema.js";
import { SCOPE, SERVER_URL } from "./test-constants.js";
import type { Db } from "jazz-tools";
import type { SessionOverrides } from "../../src/account.js";

// These tests are about the local-first client (IndexedDB, sync between replicas).
const loadLocalFirst = () => import("jazz-tools/local-first");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
  const instances: Array<{ container: HTMLDivElement; db: Db; destroy: () => Promise<void> }> = [];

  /** Mount the app into a fresh container. */
  /** Each mount is its own user unless it shares a `store` with another. */
  async function mount(config?: SessionOverrides): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);

    const { db, destroy } = await startApp(el, {
      appId: SCOPE,
      store: memoryAccountStore(),
      client: loadLocalFirst,
      localFirst: "now",
      driver: { type: "persistent", dbName: crypto.randomUUID() },
      ...config,
    });
    instances.push({ container: el, db, destroy });

    // Wait for the app to render
    await waitFor(() => el.querySelector("#todo-list") !== null, 5000, "App should render");
    await waitFor(
      () => {
        const submitButton = el.querySelector<HTMLButtonElement>('#add-form button[type="submit"]');
        return submitButton !== null && !submitButton.disabled;
      },
      5000,
      "Todo form should be ready",
    );

    return el;
  }

  /** Destroy a specific instance. */
  async function destroyInstance(el: HTMLDivElement): Promise<void> {
    const idx = instances.findIndex((i) => i.container === el);
    if (idx === -1) return;
    await instances[idx].destroy();
    el.remove();
    instances.splice(idx, 1);
  }

  afterEach(async () => {
    while (instances.length > 0) {
      await destroyInstance(instances[0].container);
    }
  });

  todoCrudTest({ it, expect, mount, waitFor, submit: addTodo });

  it("renders todo titles and descriptions as text instead of HTML", async () => {
    const el = await mount();
    const instance = instances.find(({ container }) => container === el);
    if (!instance) throw new Error("Mounted todo test database is unavailable");
    const title = '<img src="invalid" data-injected-title="true">';
    const description = '<img src="invalid" data-injected-description="true">';

    addTodo(el, "Initial title");

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear after submission",
    );

    const todoId = el.querySelector<HTMLSelectElement>("#parent-select")?.options[1]?.value;
    if (!todoId) throw new Error("Submitted todo should be selectable as a parent");
    instance.db.update(app.todos, todoId, { title, description });

    await waitFor(
      () => el.querySelector("#todo-list li span")?.textContent === title,
      3000,
      "Updated todo should render",
    );

    expect(el.querySelector("#todo-list li span")?.textContent).toBe(title);
    expect(el.querySelector("#todo-list li small")?.textContent).toBe(description);
    expect(el.querySelector("[data-injected-title], [data-injected-description]")).toBeNull();
  });

  it("renders child todos directly under their parent with nesting depth", async () => {
    const el = await mount();

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

  it("persists todos across app destroy and remount (IndexedDB)", async () => {
    const dbName = crypto.randomUUID();
    const store = memoryAccountStore();

    // First session: mount, add todo, destroy
    const el1 = await mount({ store, driver: { type: "persistent", dbName } });
    addTodo(el1, "Survive reload");

    await waitFor(
      () => el1.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear in first session",
    );

    await destroyInstance(el1);

    // Second session: remount with same store and dbName — IndexedDB data should load
    const el2 = await mount({ store, driver: { type: "persistent", dbName } });

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      5000,
      "Todo should survive remount from IndexedDB",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Survive reload");
  });

  it("syncs a todo between two app instances through the server", async () => {
    const serverUrl = SERVER_URL;

    const el1 = await mount({
      appId: SCOPE,
      serverUrl,
    });
    const el2 = await mount({
      appId: SCOPE,
      serverUrl,
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
