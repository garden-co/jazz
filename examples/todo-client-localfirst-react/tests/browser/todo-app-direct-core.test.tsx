/**
 * Direct-core browser canary for the React todo app.
 *
 * Mounts the real <App /> component in Chromium and connects two persistent
 * OPFS clients to one local Jazz server. The public serverUrl config is
 * converted by the runtime to the direct websocket endpoint.
 */

import { describe, it, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { APP_ID, ADMIN_SECRET, SERVER_URL } from "./test-constants.js";
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

function todoItemByTitle(el: HTMLDivElement, title: string): HTMLLIElement | null {
  return (
    [...el.querySelectorAll<HTMLLIElement>("#todo-list li")].find(
      (li) => li.querySelector("span")?.textContent === title,
    ) ?? null
  );
}

async function addTodo(el: HTMLDivElement, title: string): Promise<void> {
  const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
  const form = input.closest("form")!;

  await act(async () => {
    typeInto(input, title);
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  });
}

describe("React Todo App direct-core browser canary", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(config: {
    appId?: string;
    serverUrl?: string;
    secret?: string;
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

    await waitFor(
      () => el.querySelector("#todo-list") !== null,
      5000,
      "App should render the todo list",
    );

    return el;
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

  it("syncs two persistent OPFS app instances through one direct-core server", async () => {
    const writerDbName = uniqueDbName("direct-core-writer");
    const readerDbName = uniqueDbName("direct-core-reader");

    const writer = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: writerDbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: "Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs",
    });
    const reader = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: readerDbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: "VDOGX2nez-5T9Lgk4VfYMT33Qsa6J4loRAoKLZpvxBg",
    });

    await new Promise((r) => setTimeout(r, 750));

    await addTodo(writer, "Direct-core writer todo");
    await waitFor(
      () => hasTodoTitle(reader, "Direct-core writer todo"),
      20000,
      "reader useAll subscription should observe writer create",
    );

    const writerItem = todoItemByTitle(writer, "Direct-core writer todo")!;
    await act(async () => writerItem.querySelector<HTMLInputElement>("input.toggle")!.click());

    await waitFor(
      () => todoItemByTitle(writer, "Direct-core writer todo")?.classList.contains("done") === true,
      3000,
      "writer should render its own update before remount",
    );

    await waitFor(
      () => todoItemByTitle(reader, "Direct-core writer todo")?.classList.contains("done") === true,
      20000,
      "reader useAll subscription should observe writer update",
    );

    await addTodo(reader, "Direct-core reader todo");
    await waitFor(
      () => hasTodoTitle(writer, "Direct-core reader todo"),
      20000,
      "writer useAll subscription should observe reader create",
    );

    const deleteButton = todoItemByTitle(
      writer,
      "Direct-core writer todo",
    )!.querySelector<HTMLButtonElement>(".delete-btn")!;
    await act(async () => deleteButton.click());

    await waitFor(
      () => !hasTodoTitle(reader, "Direct-core writer todo"),
      20000,
      "reader useAll subscription should observe writer delete",
    );
  });

  it.skip("reopens a persistent OPFS app instance with DOM-written todos", async () => {
    // Blocked: the existing app-level OPFS remount test in todo-app.test.tsx
    // currently times out after a DOM insert, and TodoList does not expose the
    // public WriteHandle needed to wait for local durability before unmount.
    // Keep the skipped canary here so the direct-core browser gate records the
    // missing reopen coverage without silently exercising server replay instead.
  });
});
