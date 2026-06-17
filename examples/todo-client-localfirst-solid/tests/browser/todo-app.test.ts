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

describe("Solid Todo App E2E", () => {
  const mounts: Array<{ dispose: () => void; container: HTMLDivElement }> = [];

  async function mountApp(config: Partial<DbConfig>): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);

    const dispose = render(
      () => App({ config: { appId: config.appId ?? "test-app", ...config } }),
      el,
    );
    mounts.push({ dispose, container: el });

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

    mounts[idx].dispose();
    el.remove();
    mounts.splice(idx, 1);
    await new Promise((r) => setTimeout(r, 200));
  }

  afterEach(async () => {
    for (const { dispose, container } of mounts) {
      try {
        dispose();
      } catch {
        // best effort
      }
      container.remove();
    }
    mounts.length = 0;
  });

  it("renders the app with an empty todo list", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("empty") } });

    expect(el.querySelector("h1")!.textContent).toBe("Todos");
    expect(el.querySelector("#todo-list")).toBeTruthy();
    expect(el.querySelectorAll("#todo-list li").length).toBe(0);
  });

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

  it("toggles a todo's done state via checkbox", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("toggle") } });

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

    const checkbox = li.querySelector<HTMLInputElement>("input.toggle")!;
    checkbox.click();

    await waitFor(
      () => el.querySelector("#todo-list li")!.classList.contains("done"),
      3000,
      "Todo should be marked done",
    );
  });

  it("deletes a todo via the delete button", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("delete") } });

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

    const deleteBtn = el.querySelector<HTMLButtonElement>(".delete-btn")!;
    deleteBtn.click();

    await waitFor(
      () => el.querySelectorAll("#todo-list li").length === 0,
      3000,
      "Todo should be removed",
    );
  });

  it("renders multiple todos with correct state", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("multi") } });

    const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
    const form = input.closest("form")!;

    for (const title of ["First", "Second", "Third"]) {
      input.value = title;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
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

  it("persists todos across app unmount and remount (OPFS)", async () => {
    const dbName = uniqueDbName("opfs");

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

    await unmountApp(el1);

    const el2 = await mountApp({ driver: { type: "persistent", dbName } });

    await waitFor(
      () => el2.querySelectorAll("#todo-list li").length === 1,
      5000,
      "Todo should survive remount from OPFS",
    );

    expect(el2.querySelector("#todo-list li span")!.textContent).toBe("Survive reload");
  });

  it("syncs a todo between two app instances through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    const el1 = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-a") },
      serverUrl,
    });
    const el2 = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: uniqueDbName("sync-b") },
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
