/**
 * E2E browser tests for the Svelte todo app.
 *
 * Mounts the real App component in a Chromium browser via @vitest/browser + playwright.
 * Interacts through the actual DOM the app renders — no test-only components.
 */

import { describe, it, expect, afterEach } from "vitest";
import { mount, unmount, type Component } from "svelte";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Svelte Todo App E2E", () => {
  const mounts: Array<{ instance: Record<string, never>; container: HTMLDivElement }> = [];

  /** Mount the real App. Returns the container element. */
  async function mountApp(config: Partial<DbConfig>): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);

    // Dynamic import so the Svelte compiler processes the component
    const { default: App } = await import("../../src/App.svelte");

    const instance = mount(App as Component, {
      target: el,
      props: { config: { appId: config.appId ?? "test-app", ...config } },
    });
    mounts.push({ instance, container: el });

    // Wait for JazzSvelteProvider to initialise and TodoList to render
    await waitFor(
      () => el.querySelector("#todo-list") !== null,
      5000,
      "App should render the todo list",
    );

    return el;
  }

  /** Unmount a specific app instance (triggers JazzSvelteProvider shutdown). */
  async function unmountApp(el: HTMLDivElement): Promise<void> {
    const idx = mounts.findIndex((m) => m.container === el);
    if (idx === -1) return;
    const { instance } = mounts[idx];
    unmount(instance);
    el.remove();
    mounts.splice(idx, 1);
    // Give OPFS handles time to release
    await new Promise((r) => setTimeout(r, 200));
  }

  afterEach(async () => {
    for (const { instance, container } of mounts) {
      try {
        unmount(instance);
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

    input.value = "Buy milk";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));

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
    input.value = "Toggle me";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear",
    );

    const li = el.querySelector("#todo-list li")!;
    expect(li.classList.contains("done")).toBe(false);

    // Click the checkbox
    const checkbox = li.querySelector<HTMLInputElement>("input.toggle")!;
    checkbox.click();

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
    input.value = "Delete me";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear",
    );

    // Click the delete button
    const deleteBtn = el.querySelector<HTMLButtonElement>(".delete-btn")!;
    deleteBtn.click();

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
      input.value = title;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      // Brief pause so each insert completes
      await new Promise((r) => setTimeout(r, 100));
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

    input1.value = "Survive reload";
    input1.dispatchEvent(new Event("input", { bubbles: true }));
    form1.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));

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
  // 7. Server sync between two app instances
  // -------------------------------------------------------------------------

  // Longer timeout: sync can take up to 20s under full-suite load.
  it("syncs a todo between two app instances through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    // Mount two independent app instances connected to the same server
    const el1 = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-a") },
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: "svelte-sync-user-a",
      adminSecret: ADMIN_SECRET,
    });
    const el2 = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-b") },
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: "svelte-sync-user-b",
      adminSecret: ADMIN_SECRET,
    });

    // Let both app instances finish server/event-stream setup before mutating.
    await new Promise((r) => setTimeout(r, 750));

    // Add a todo in app 1 via the form
    const input1 = el1.querySelector<HTMLInputElement>("input[type='text']")!;
    const form1 = input1.closest("form")!;

    input1.value = "Synced todo";
    input1.dispatchEvent(new Event("input", { bubbles: true }));
    form1.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));

    await waitFor(
      () => el1.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear in app 1",
    );

    // Wait for it to appear in app 2 via server sync
    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      25000,
      "Todo should sync to app 2 through the server",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Synced todo");
  }, 60000);
});
