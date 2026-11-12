import { todoCrudTest } from "../../../../testing/todo-app-scenarios.js";
import type { AccountStore, Db } from "jazz-tools";
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
import type { SessionOverrides } from "../../src/account.js";

/** Device accounts kept in memory: a store per mount is a new account, a shared one reopens it. */
function memoryAccountStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

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

/**
 * The list row showing this title, if any. Every guest can read every todo
 * here, and all tests share one server, so a local-first client also shows
 * other tests' todos: find this test's own rows, not a row count.
 */
function rowFor(el: HTMLElement, title: string): HTMLLIElement | undefined {
  return [...el.querySelectorAll<HTMLLIElement>("#todo-list li")].find(
    (li) => li.querySelector("span")?.textContent === title,
  );
}

/** A title no other test uses. */
function unique(title: string): string {
  return `${title} ${crypto.randomUUID()}`;
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
  type MountOptions = SessionOverrides & { secret?: string };

  async function mount(config?: MountOptions): Promise<HTMLDivElement> {
    const { container } = await mountWithDb(config);
    return container;
  }

  /** Mount the app into a fresh container and expose its public Db API. */
  async function mountWithDb(
    config?: MountOptions,
  ): Promise<{ container: HTMLDivElement; db: Db }> {
    const el = document.createElement("div");
    document.body.appendChild(el);

    const { secret, ...overrides } = config ?? {};
    const { session, destroy } = await startApp(el, {
      appId: SCOPE,
      serverUrl: SERVER_URL,
      driver: { type: "persistent", dbName: crypto.randomUUID() },
      store: memoryAccountStore(),
      // Open local-first from the first render: these tests check local
      // persistence, which the thin client doesn't keep.
      client: () => import("jazz-tools/local-first"),
      localFirst: "now",
      ...overrides,
      // A fixed key restores that device account; otherwise each store gets a new one.
      ...(secret === undefined ? {} : { initial: "signed-out" as const }),
    });
    instances.push({ container: el, destroy });
    if (secret !== undefined) await session.restoreDeviceAccount(secret);
    const db = session.db;

    // Wait for the app to render
    await waitFor(() => el.querySelector("#todo-list") !== null, 5000, "App should render");

    return { container: el, db };
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
    const { container: el, db } = await mountWithDb();
    const title = '<img src="invalid" data-injected-title="true">';
    const description = '<img src="invalid" data-injected-description="true">';

    const initial = unique("Initial title");
    addTodo(el, initial);

    await waitFor(
      () => rowFor(el, initial) !== undefined,
      3000,
      "Todo should appear after submission",
    );

    const todoId = [...el.querySelector<HTMLSelectElement>("#parent-select")!.options].find(
      (option) => option.textContent === initial,
    )?.value;
    if (!todoId) throw new Error("Submitted todo should be selectable as a parent");
    db.update(app.todos, todoId, { title, description });

    await waitFor(() => rowFor(el, title) !== undefined, 3000, "Updated todo should render");

    expect(rowFor(el, title)!.querySelector("small")?.textContent).toBe(description);
    expect(el.querySelector("[data-injected-title], [data-injected-description]")).toBeNull();
  });

  it("renders child todos directly under their parent with nesting depth", async () => {
    const el = await mount();

    const parent = unique("Parent task");
    const child = unique("Child task");
    addTodo(el, parent);

    await waitFor(() => rowFor(el, parent) !== undefined, 3000, "Parent todo should appear");

    addTodoWithParent(el, child, parent);

    await waitFor(() => rowFor(el, child) !== undefined, 3000, "Child todo should appear");

    // The child renders directly under its parent, one level deeper.
    const parentRow = rowFor(el, parent)!;
    expect(parentRow.nextElementSibling).toBe(rowFor(el, child));
    expect(parentRow.dataset.depth).toBe("0");
    expect(rowFor(el, child)!.dataset.depth).toBe("1");
  });

  it("persists todos across app destroy and remount (IndexedDB)", async () => {
    const dbName = crypto.randomUUID();
    const title = unique("Survive reload");
    // One store: the second mount reopens the first one's device account.
    const store = memoryAccountStore();

    // First session: mount, add todo, destroy
    const el1 = await mount({ store, driver: { type: "persistent", dbName } });
    addTodo(el1, title);

    await waitFor(
      () => rowFor(el1, title) !== undefined,
      3000,
      "Todo should appear in first session",
    );

    await destroyInstance(el1);

    // Second session: remount with same dbName — IndexedDB data should load
    const el2 = await mount({ store, driver: { type: "persistent", dbName } });

    await waitFor(
      () => rowFor(el2, title) !== undefined,
      5000,
      "Todo should survive remount from IndexedDB",
    );
  });

  it("syncs a todo between two app instances through the server", async () => {
    const serverUrl = SERVER_URL;

    const { container: el1, db: db1 } = await mountWithDb({
      appId: SCOPE,
      serverUrl,
      secret: "jazz-auth-v1:IsHiz7lWH1KJEuM5J8Hn_oleBb6SBcuGSE9Ro3H0G68",
    });
    const { container: el2, db: db2 } = await mountWithDb({
      appId: SCOPE,
      serverUrl,
      secret: "jazz-auth-v1:C5-etNr9-YLchXK15XLhDVIn-An8mgb35sc5lfJpAQE",
    });

    await waitFor(
      () => db1.getAuthState().authMode === "local-first",
      5000,
      "App 1 should reach local-first auth",
    );
    await waitFor(
      () => db2.getAuthState().authMode === "local-first",
      5000,
      "App 2 should reach local-first auth",
    );
    expect(db1.getAuthState().session?.authMode).toBe("local-first");
    expect(db2.getAuthState().session?.authMode).toBe("local-first");

    // Let both app instances finish server/event-stream setup before mutating.
    await new Promise((r) => setTimeout(r, 750));

    // Add a todo in app 1
    const title = unique("Synced todo");
    addTodo(el1, title);

    await waitFor(() => rowFor(el1, title) !== undefined, 3000, "Todo should appear in app 1");

    // Wait for it to appear in app 2 via server sync
    await waitFor(
      () => rowFor(el2, title) !== undefined,
      10000,
      "Todo should sync to app 2 through the server",
    );
  });
});
