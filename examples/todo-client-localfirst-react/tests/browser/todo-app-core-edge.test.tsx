/**
 * E2E browser tests for the React todo app.
 *
 * Mounts the real <App /> component in a Chromium browser via @vitest/browser + playwright.
 * Interacts through the actual DOM the app renders — no test-only components.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { APP_ID, ADMIN_SECRET, EDGE_SERVER_URL, CORE_SERVER_URL } from "./test-constants.js";
import type { DbConfig } from "jazz-tools";

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

/** Type into an input controlled by React (triggers React's onChange). */
function typeInto(input: HTMLInputElement, value: string) {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!;
  setter.call(input, value);
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

function todoTitles(el: HTMLDivElement): Array<string | null> {
  return [...el.querySelectorAll("#todo-list li span")].map((span) => span.textContent);
}

function hasTodoTitle(el: HTMLDivElement, title: string): boolean {
  return todoTitles(el).includes(title);
}

async function addTodo(el: HTMLDivElement, title: string): Promise<void> {
  const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
  const form = input.closest("form")!;

  await act(async () => {
    typeInto(input, title);
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("React Todo App E2E", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  /** Mount the real App. Returns the container element. */
  async function mountApp(config: {
    appId?: string;
    serverUrl?: string;
    auth?: { localFirstSecret: string };
    adminSecret?: string;
    driver?: DbConfig["driver"];
  }): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    await act(async () => {
      r.render(<App config={{ appId: config.appId ?? "test-app", ...config }} />);
    });

    // Wait for JazzProvider to initialize and TodoList to render
    await waitFor(
      () => el.querySelector("#todo-list") !== null,
      5000,
      "App should render the todo list",
    );

    return el;
  }

  /** Unmount a specific app instance (triggers JazzProvider shutdown). */
  async function unmountApp(el: HTMLDivElement): Promise<void> {
    const idx = mounts.findIndex((m) => m.container === el);
    if (idx === -1) return;
    const { root } = mounts[idx];
    await act(async () => root.unmount());
    el.remove();
    mounts.splice(idx, 1);
    // Give OPFS handles time to release
    await new Promise((r) => setTimeout(r, 200));
  }

  afterEach(async () => {
    for (const { root, container } of mounts) {
      try {
        await act(async () => root.unmount());
      } catch {
        /* best effort */
      }
      container.remove();
    }
    mounts.length = 0;
  });

  // -------------------------------------------------------------------------
  // 1. App renders with empty list
  // -------------------------------------------------------------------------

  it("renders the app with an empty todo list", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("empty") } });

    expect(el.querySelector("h1")!.textContent).toBe("Todos");
    expect(el.querySelector("#todo-list")).toBeTruthy();
    expect(el.querySelectorAll("#todo-list li").length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 2. Add todo via form
  // -------------------------------------------------------------------------

  it("adds a todo via the form", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("add") } });

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
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("toggle") } });

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
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("delete") } });

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
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("multi") } });

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

  it("persists todos across app unmount and remount (OPFS)", async () => {
    const dbName = uniqueDbName("opfs");

    // First session: mount app, add a todo via the form
    const el1 = await mountApp({ driver: { type: "persistent", dbName } });
    const input1 = el1.querySelector<HTMLInputElement>("input[type='text']")!;
    const form1 = input1.closest("form")!;

    await act(async () => {
      typeInto(input1, "Survive reload");
      form1.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });

    await waitFor(
      () => el1.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear in first session",
    );

    // Unmount (triggers db.shutdown, flushes OPFS)
    await unmountApp(el1);

    // Second session: remount with same dbName — OPFS data should load
    const el2 = await mountApp({ driver: { type: "persistent", dbName } });

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      5000,
      "Todo should survive remount from OPFS",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Survive reload");
  });

  // -------------------------------------------------------------------------
  // 7. Core-edge sync between two app instances
  // -------------------------------------------------------------------------

  it("syncs todos between app instances connected to the edge and core", async () => {
    const edgeApp = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-c") },
      serverUrl: EDGE_SERVER_URL,
      adminSecret: ADMIN_SECRET,
      auth: { localFirstSecret: "Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs" },
    });
    const coreApp = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-d") },
      serverUrl: CORE_SERVER_URL,
      adminSecret: ADMIN_SECRET,
      auth: { localFirstSecret: "VDOGX2nez-5T9Lgk4VfYMT33Qsa6J4loRAoKLZpvxBg" },
    });

    // Let both app instances finish server/event-stream setup before mutating.
    await new Promise((r) => setTimeout(r, 750));

    await addTodo(edgeApp, "Edge to core todo");

    await waitFor(
      () => hasTodoTitle(edgeApp, "Edge to core todo"),
      3000,
      "Todo should appear on edge app",
    );

    await waitFor(
      () => hasTodoTitle(coreApp, "Edge to core todo"),
      20000,
      "Edge todo should sync to the core app",
    );

    await addTodo(coreApp, "Core to edge todo");

    await waitFor(
      () => hasTodoTitle(coreApp, "Core to edge todo"),
      3000,
      "Todo should appear on core app",
    );

    await waitFor(
      () => hasTodoTitle(edgeApp, "Core to edge todo"),
      20000,
      "Core todo should sync to the edge app",
    );
  });

  // -------------------------------------------------------------------------
  // 8. Core-edge sync between two app instances with memory driver
  // -------------------------------------------------------------------------

  it("syncs a todo between core and edge app instances without local persistence", async () => {
    const edgeApp = await mountApp({
      appId: APP_ID,
      serverUrl: EDGE_SERVER_URL,
      auth: { localFirstSecret: "disAKUpEX273joMo4f1NTW-tDTpc4bzPy_l5tvNLXnc" },
      driver: { type: "memory" },
    });
    const coreApp = await mountApp({
      appId: APP_ID,
      serverUrl: CORE_SERVER_URL,
      auth: { localFirstSecret: "TqNBXTv_Mv7HBp3FZ6KtHJwBWvnkI7YcOlrS57d3eEs" },
      driver: { type: "memory" },
    });

    // Let both app instances finish server/event-stream setup before mutating.
    await new Promise((r) => setTimeout(r, 750));

    await addTodo(edgeApp, "Inmemory todo");

    await waitFor(
      () => hasTodoTitle(edgeApp, "Inmemory todo"),
      3000,
      "Todo should appear in edge app",
    );

    await waitFor(
      () => hasTodoTitle(coreApp, "Inmemory todo"),
      20000,
      "Todo should sync from edge app to core app",
    );
  });
});
