import { todoCrudTest } from "../../../testing/todo-app-scenarios.js";
import { createAccountManager } from "jazz-tools";
import { prepareTestAccount } from "../../../testing/accounts.js";
/**
 * E2E browser tests for the Solid todo app.
 *
 * Mounts the real <App /> in Chromium via @vitest/browser + playwright.
 */

import { describe, it, expect, afterEach } from "vitest";
import { render } from "solid-js/web";
import { App } from "../../src/App.js";
import { TEST_PORT, APP_ID } from "./test-constants.js";
import type { DbConfig } from "jazz-tools";

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

describe("Solid Todo App E2E", () => {
  const mounts: Array<{
    dispose: () => void;
    container: HTMLDivElement;
    storageNamespace: string | undefined;
  }> = [];

  async function mountApp(config: Partial<DbConfig> = {}): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
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

    const dispose = render(() => App({ config: resolvedConfig }), el);
    mounts.push({ dispose, container: el, storageNamespace });

    await waitFor(
      () => el.querySelector("#todo-list") !== null,
      5000,
      "App should render the todo list",
    );

    return el;
  }

  async function unmountApp(el: HTMLDivElement): Promise<void> {
    const idx = mounts.findIndex((m) => m.container === el);
    if (idx === -1) return;

    const { dispose, storageNamespace } = mounts[idx];
    dispose();
    el.remove();
    mounts.splice(idx, 1);

    if (storageNamespace) {
      const storage = (
        window as Window & {
          __jazz?: {
            shutdown(namespace?: string): Promise<void>;
            listLiveStorageNamespaces(): string[];
          };
        }
      ).__jazz;
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

    await unmountApp(el1);

    const el2 = await mountApp({ account, driver: { type: "persistent", dbName } });

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      5000,
      "Todo should survive remount from IndexedDB",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Survive reload");
  });

  it("syncs a todo between two app instances through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    const el1 = await mountApp({
      appId: APP_ID,
      serverUrl,
    });
    const el2 = await mountApp({
      appId: APP_ID,
      serverUrl,
    });

    await new Promise((r) => setTimeout(r, 750));

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

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      25000,
      "Todo should sync to app 2 through the server",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Synced todo");
  }, 60000);
});
