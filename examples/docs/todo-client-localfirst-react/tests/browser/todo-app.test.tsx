import { todoCrudTest } from "../../../../testing/todo-app-scenarios.js";
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
import { SCOPE, SERVER_URL } from "./test-constants.js";
import type { AccountStore } from "jazz-tools";
import { createJazzClient, type JazzClient, type JazzSession } from "jazz-tools/react";
import { sessionConfig, type SessionOverrides } from "../../src/account.js";
import { memoryAccountStore } from "./memory-account-store.js";

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
  const mounts: Array<{
    root: Root;
    container: HTMLDivElement;
    storageNamespace: string | undefined;
    session: JazzSession<JazzClient> | undefined;
  }> = [];

  /**
   * Mount the real App on its own device account (a fresh one unless `store`
   * holds one), or on the device account `deviceKey` restores.
   */
  async function mountApp(
    config: {
      appId?: string;
      serverUrl?: string;
      deviceKey?: string;
      store?: AccountStore;
      driver?: SessionOverrides["driver"];
    } = {},
  ): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    const { deviceKey, ...overrides } = config;
    const resolvedConfig: SessionOverrides = {
      appId: SCOPE,
      store: memoryAccountStore(),
      driver: { type: "persistent", dbName: crypto.randomUUID() },
      ...overrides,
    };
    // A fixed device key: open signed out, then restore that account.
    let session: JazzSession<JazzClient> | undefined;
    if (deviceKey !== undefined) {
      session = await createJazzClient(sessionConfig({ ...resolvedConfig, initial: "signed-out" }));
      await session.restoreDeviceAccount(deviceKey);
    }
    const storageNamespace =
      resolvedConfig.driver?.type === "persistent" ? resolvedConfig.driver.dbName : undefined;
    mounts.push({ root: r, container: el, storageNamespace, session });

    await act(async () => {
      r.render(session ? <App session={session} /> : <App config={resolvedConfig} />);
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
    const { root, storageNamespace, session } = mounts[idx];
    await act(async () => root.unmount());
    // The provider doesn't close a session it was given.
    await session?.close();
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

  todoCrudTest({
    it,
    expect,
    mount: mountApp,
    waitFor,
    submit: (el, title) =>
      act(async () => {
        const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
        typeInto(input, title);
        input
          .closest("form")!
          .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      }),
    click: (el) =>
      act(async () => {
        el.click();
      }),
  });

  it("persists todos across app unmount and remount (IndexedDB)", async () => {
    const dbName = crypto.randomUUID();
    // One store: the second mount reopens the same device account.
    const store = memoryAccountStore();

    // First session: mount app, add a todo via the form
    const el1 = await mountApp({ store, driver: { type: "persistent", dbName } });
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

    // Unmount (triggers db.shutdown, flushes IndexedDB)
    await unmountApp(el1);

    // Second session: remount with same dbName — IndexedDB data should load
    const el2 = await mountApp({ store, driver: { type: "persistent", dbName } });

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      5000,
      "Todo should survive remount from IndexedDB",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Survive reload");
  });

  it("syncs a todo between two app instances through the server", async () => {
    (globalThis as { __JAZZ_SUBSCRIPTION_TRACE__?: boolean }).__JAZZ_SUBSCRIPTION_TRACE__ = true;
    const serverUrl = SERVER_URL;

    // Mount two independent app instances connected to the same server
    const el1 = await mountApp({
      appId: SCOPE,
      serverUrl,
      deviceKey: "jazz-auth-v1:Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs",
    });
    const el2 = await mountApp({
      appId: SCOPE,
      serverUrl,
      deviceKey: "jazz-auth-v1:VDOGX2nez-5T9Lgk4VfYMT33Qsa6J4loRAoKLZpvxBg",
    });

    // Let both app instances finish server/event-stream setup before mutating.
    await new Promise((r) => setTimeout(r, 750));

    // Add a todo in app 1 via the form
    const input1 = el1.querySelector<HTMLInputElement>("input[type='text']")!;
    const form1 = input1.closest("form")!;

    await act(async () => {
      typeInto(input1, "Synced todo");
      form1.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });

    await waitFor(
      () => el1.querySelectorAll("#todo-list li").length === 1,
      3000,
      "Todo should appear in app 1",
    );

    // Wait for it to appear in app 2 via server sync
    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      20000,
      "Todo should sync to app 2 through the server",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Synced todo");
  });

  it("syncs a todo between two app instances through the server without local persistence", async () => {
    const serverUrl = SERVER_URL;

    // Mount two independent app instances connected to the same server
    const el1 = await mountApp({
      appId: SCOPE,
      serverUrl,
      deviceKey: "jazz-auth-v1:disAKUpEX273joMo4f1NTW-tDTpc4bzPy_l5tvNLXnc",
      driver: { type: "memory" },
    });
    const el2 = await mountApp({
      appId: SCOPE,
      serverUrl,
      deviceKey: "jazz-auth-v1:TqNBXTv_Mv7HBp3FZ6KtHJwBWvnkI7YcOlrS57d3eEs",
      driver: { type: "memory" },
    });

    // Let both app instances finish server/event-stream setup before mutating.
    await new Promise((r) => setTimeout(r, 750));

    // Add a todo in app 1 via the form
    const input1 = el1.querySelector<HTMLInputElement>("input[type='text']")!;
    const form1 = input1.closest("form")!;

    await act(async () => {
      typeInto(input1, "Inmemory todo");
      form1.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });

    await waitFor(
      () =>
        Array.from(el1.querySelectorAll("#todo-list li span").values()).some(
          (el) => el.textContent === "Inmemory todo",
        ),
      3000,
      "Todo should appear in app 1",
    );

    // Wait for it to appear in app 2 via server sync
    await waitFor(
      () =>
        Array.from(el2.querySelectorAll("#todo-list li span").values()).some(
          (el) => el.textContent === "Inmemory todo",
        ),
      20000,
      "Todo should sync to app 2 through the server",
    );
  });
});
