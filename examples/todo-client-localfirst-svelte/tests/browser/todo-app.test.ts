import { todoCrudTest } from "../../../testing/todo-app-scenarios.js";
/**
 * E2E browser tests for the Svelte todo app.
 *
 * Mounts the real App component in a Chromium browser via @vitest/browser + playwright.
 * Interacts through the actual DOM the app renders — no test-only components.
 */

import { describe, it, expect, afterEach } from "vitest";
import { mount, unmount, type Component } from "svelte";
import { TEST_PORT, APP_ID } from "./test-constants.js";
import { createAccountManager, type DbConfig } from "jazz-tools";
import { prepareTestAccount } from "../../../testing/accounts.js";

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Svelte Todo App E2E", () => {
  const mounts: Array<{
    instance: Record<string, never>;
    container: HTMLDivElement;
    storageNamespace: string | undefined;
  }> = [];

  /** Mount the real App. Returns the container element. */
  async function mountApp(config: Partial<DbConfig> = {}): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);

    // Dynamic import so the Svelte compiler processes the component
    const { default: App } = await import("../../src/App.svelte");

    const account =
      config.account ??
      (await prepareTestAccount(
        createAccountManager,
        config.appId ?? "test-app",
        config.serverUrl ?? `http://127.0.0.1:${TEST_PORT}`,
      ));
    const resolvedConfig: Partial<DbConfig> = {
      appId: config.appId ?? "test-app",
      driver: { type: "persistent", dbName: crypto.randomUUID() },
      ...config,
      account,
    };
    const storageNamespace =
      resolvedConfig.driver?.type === "persistent" ? resolvedConfig.driver.dbName : undefined;
    const instance = mount(App as Component, {
      target: el,
      props: { config: resolvedConfig },
    });
    mounts.push({ instance, container: el, storageNamespace });

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
    const { instance, storageNamespace } = mounts[idx];
    await unmount(instance);
    el.remove();
    mounts.splice(idx, 1);

    if (storageNamespace) {
      const storage = window.__jazz;
      if (!storage) throw new Error("Jazz browser storage controls are unavailable");
      await storage.shutdown(storageNamespace);
      await waitFor(
        () => !storage.listLiveStorageNamespaces().includes(storageNamespace),
        5000,
        "Unmounted Jazz storage should finish releasing",
      );
    }
  }

  afterEach(async () => {
    while (mounts.length > 0) {
      await unmountApp(mounts[0].container);
    }
  });

  todoCrudTest({ it, expect, mount: mountApp, waitFor });

  it("persists todos across app unmount and remount (IndexedDB)", async () => {
    const dbName = crypto.randomUUID();
    const account = await prepareTestAccount(
      createAccountManager,
      "test-app",
      `http://127.0.0.1:${TEST_PORT}`,
    );

    // First session: mount app, add a todo via the form
    const el1 = await mountApp({ account, driver: { type: "persistent", dbName } });
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

    // Unmount (triggers db.shutdown, flushes IndexedDB)
    await unmountApp(el1);

    // Second session: remount with same dbName — IndexedDB data should load
    const el2 = await mountApp({ account, driver: { type: "persistent", dbName } });

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      5000,
      "Todo should survive remount from IndexedDB",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Survive reload");
  });

  // Longer timeout: sync can take up to 20s under full-suite load.
  it("syncs a todo between two app instances through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    // Mount two independent app instances connected to the same server
    const el1 = await mountApp({
      appId: APP_ID,
      serverUrl,
    });
    const el2 = await mountApp({
      appId: APP_ID,
      serverUrl,
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
